/*
 * Copyright (c) 1998-2023 Cloud Software Group, Inc.
 * All Rights Reserved.
 */

/*
 * TIB/Rendezvous server Program
 *
 * This program will answer trivial request from tibrvclient programs.
 * It uses a dispatch loop in a single thread.
 *
 * This server example uses a transport enabled for direct communication
 * by default.  If the client also uses an enabled transport, and the
 * network path does not cross through RVRDs, the resulting requests and
 * replies will use direct communication instead of passing through
 * Rendezvous daemons.
 *
 * Optionally the user may specify transport parameters for the
 * communications used by the server application, and a status display
 * frequency value.  If none are specified, default values are used.
 * For information on standard default values for the transport parameters,
 * please see the TIBCO Rendezvous Concepts manual.
 *
 * The following non-standard defaults are used in this sample program:
 *   service         "7522:7523"    service for search & client requests
 *   status          0              optional frequency of status display
 *                                  counts -- if non-zero, a message is
 *                                  printed every <n> response messages
 *                                  sent.
 *
 * Examples:
 *
 *   Accept server messages on service 7500, report status every 5000 messages:
 *     tibrvserver -service 7500 -status 5000
 *
 *   Use an ephemeral port for direct communication, specify a daemon host and
 *   port to prevent autostarting a daemon, with status every 1000 requests.
 *   If both client and server use this daemon value (with no other Rendezvous
 *   application which would restart the daemon) with direct-enabled transports,
 *   you can stop the daemon and observe that messages continue between the
 *   client and server with no daemon running.
 *     tibrvserver -service 7522: -daemon localhost:7500 -status 1000
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "tibrv/tibrv.h"


#define DEFAULT_SERVICE     "7522:7523"     /* Two-part service parameter
                                               for direct communication.  To
                                               use ephemeral ports, specify
                                               in the form "7522:" */
#define DEFAULT_NETWORK     NULL
#define DEFAULT_DAEMON      NULL

#define DEFAULT_STATUS_FRQ  0               /* Default freq of status display */

#define SEARCH_SUBJECT "TIBRV.LOCATE"       /* Clients search for servers
                                               using this subject. */

#define SERVER_TIMEOUT      120.            /* Server times out after
                                               120 sec. */

static char*                program_name;

static tibrvTransport       transport;

static tibrv_u32            status_frq = DEFAULT_STATUS_FRQ;

static tibrvMsg             search_reply;   /* search_reply has global
                                               scope because we send
                                               the same reply to all
                                               search requests.
                                               Moreover,we create
                                               search_reply in main(),
                                               but send it from
                                               searchCallback(). */

static tibrv_bool           new_msg = TIBRV_FALSE; /* use a new message for
                                               reply if true;
                                               otherwise put sum
                                               in received msg. */

static tibrv_u32            requests = 0;

char                        localTime[TIBRVMSG_DATETIME_STRING_SIZE];
char                        gmtTime[TIBRVMSG_DATETIME_STRING_SIZE];


/* This routine lists the program parameters if the first parameter is a help
   flag (-help or -h or -?) or invalid parameters are detected. */
void
usage(void)
{
    fprintf(stderr, "tibrvserver_direct  [-service <service>] [-network <network>]\n");
    fprintf(stderr, "                    [-daemon  <daemon>]  [-status  <#msgs>]\n");
    exit(1);
}

/* This routine parses the command line. */
tibrv_u32
getParameters(int           argc,
              char*         argv[],
              char**        service,
              char**        network,
              char**        daemon,
              tibrv_u32*    status_frq)
{
    char*               progptr;
    int i = 1;

    /* Program name, possibly with directory data, is the first element. */
    program_name = argv[0];
    if (strrchr(program_name,'\\') != NULL)
    {   /* strip off directory information in \ format */
        progptr = strrchr(program_name,'\\')+1;
        strncpy(program_name, progptr,
                (strlen(program_name)-(progptr-program_name)+1));
    }
    else if (strrchr(program_name,'/') != NULL)
    {   /* strip off directory information in / format */
        progptr = strrchr(program_name,'/')+1;
        strncpy(program_name, progptr,
                (strlen(program_name)-(progptr-program_name)+1));
    }

    /* Check for a help flag as the first parameter. If the first command
       line argument is a help flag, display usage instructions and quit. */
    if (i < argc)
    {
        if ((strcmp(argv[i],"-h")==0) ||
            (strcmp(argv[i],"-help")==0) ||
            (strcmp(argv[i],"?")==0))
        {
            usage();
        }
    }

    /* Parse parameters with a following value. */
    while (((i + 2) <= argc) && (*argv[i] == '-'))
    {
        if (strcmp(argv[i], "-service") == 0)
        {
            *service = argv[i + 1];
            i += 2;
        }
        else if (strcmp(argv[i], "-network") == 0)
        {
            *network = argv[i + 1];
            i += 2;
        }
        else if (strcmp(argv[i], "-daemon") == 0)
        {
            *daemon = argv[i + 1];
            i += 2;
        }
        else if (strcmp(argv[i], "-status") == 0)
        {
            *status_frq = (tibrv_u32) atol(argv[i+1]);
            i += 2;
        }
        else
        {
            /* If there is some other parameter with a value,
               display usage instructions and quit. */
            usage();
        }
    }
    return i;   /* return the index of the next argument */
}


/* This callback is executed when a server search is received.  It sends the
   message prepared in the main routine as a reply to server query messages. */
static void
searchCallback(tibrvEvent event,
               tibrvMsg   message,
               void*      closure)
{
    tibrv_status return_code = TIBRV_OK;

    /*  Report receipt of client search request. */
    tibrvMsg_GetCurrentTimeString(localTime, gmtTime);
    fprintf(stdout, "%s: Client search message received\n", gmtTime);

    /*  Send our previously prepared reply message. */
    return_code = tibrvTransport_SendReply(transport, search_reply, message);
    if (return_code != TIBRV_OK)
    {
        fprintf(stderr,
                "%s failed to send a reply to a server search: %s\n",
                program_name,
                tibrvStatus_GetText(return_code));
        exit(1);
    }
}


/* This callback is executed when a client request is received.  It adds the
   values in the request, puts the result in a message, and sends it as
   a reply. */
static void
requestCallback(tibrvEvent event,
                tibrvMsg   message,
                void*      closure)
{
    tibrv_status    return_code = TIBRV_OK;
    tibrvMsg        request_reply;
    tibrv_u32       x, y, sum;

    /* Get the values in field "x" */
    return_code = tibrvMsg_GetU32(message, "x", &x);;
    if (return_code != TIBRV_OK)
    {
        fprintf(stderr,
                "%s failed to get the value of x: %s\n",
                program_name,
                tibrvStatus_GetText(return_code));
        exit(1);
    }

    /* Get the values in field "y" */
    return_code = tibrvMsg_GetU32(message, "y", &y);;
    if (return_code != TIBRV_OK)
    {
        fprintf(stderr,
                "%s failed to get the value of y: %s\n",
                program_name,
                tibrvStatus_GetText(return_code));
        exit(1);
    }

    /* Add the values and update the reply message with the sum. */
    sum = x + y;

    /* If new_msg is true, create a new message, add the sum as a field, send it,
       and destroy it.  If new_msg is false, update or insert the sum field and
       send.  In this case we do not destroy the message because an inbound
       message in a callback is owned by Rendezvous. */
    if (new_msg)
    {
        /* Create a new reply message. */
        return_code = tibrvMsg_Create(&request_reply);
        if (return_code != TIBRV_OK)
        {
            fprintf(stderr,
                    "%s failed to initialize a reply to a client request: %s\n",
                    program_name,
                    tibrvStatus_GetText(return_code));
            exit(1);
        }

        /* Put the sum in the reply message. */
        return_code = tibrvMsg_UpdateU32(request_reply, "sum", sum);
        if (return_code != TIBRV_OK)
        {
            fprintf(stderr,
                    "%s failed to update a reply to a client request: %s\n",
                    program_name,
                    tibrvStatus_GetText(return_code));
            exit(1);
        }

        /* Send a reply to the request message. */
        return_code = tibrvTransport_SendReply(transport, request_reply, message);
        if (return_code != TIBRV_OK)
        {
            fprintf(stderr,
                    "%s failed to send a reply to a client request: %s\n",
                    program_name,
                    tibrvStatus_GetText(return_code));
            exit(1);
        }

        /* Destroy our reply message to reclaim space. */
        return_code = tibrvMsg_Destroy(request_reply);
        if (return_code != TIBRV_OK)
        {
            fprintf(stderr,
                    "%s failed to destroy a reply to a client request: %s\n",
                    program_name,
                    tibrvStatus_GetText(return_code));
            exit(1);
        }
    }
    else
    {
        /* Put the sum in the request message received from the client. */
        return_code = tibrvMsg_UpdateU32(message, "sum", sum);
        if (return_code != TIBRV_OK)
        {
            fprintf(stderr,
                    "%s failed to update a reply to a client request: %s\n",
                    program_name,
                    tibrvStatus_GetText(return_code));
            exit(1);
        }

        /* Send the message back as aa reply to the request message. */
        return_code = tibrvTransport_SendReply(transport, message, message);
        if (return_code != TIBRV_OK)
        {
            fprintf(stderr,
                    "%s failed to send a reply to a client request: %s\n",
                    program_name,
                    tibrvStatus_GetText(return_code));
            exit(1);
        }
    }

    /* Increment the count of processed request messages.  Display if appropriate. */
    requests++;
    if (status_frq > 0) {
        if ((requests/status_frq)*status_frq == requests)
        {
            tibrvMsg_GetCurrentTimeString(localTime, gmtTime);
            fprintf(stdout,"%s: %d client requests processed\n",
                    gmtTime, requests);
        }
    }

}


/* This is the main routine. */
int
main(int    argc,
     char** argv)
{
    tibrv_status        return_code;

    tibrvEvent          search_event = 0;
    tibrvEvent          request_event = 0;

    char*               service = DEFAULT_SERVICE;
    char*               network = DEFAULT_NETWORK;
    char*               daemon  = DEFAULT_DAEMON;
    char*               search_subject = SEARCH_SUBJECT;
    char                inbox_subject[TIBRV_SUBJECT_MAX];

    const char*         tibrv_version;

     /* Parse the command line and set up the transport parameters. */
    getParameters(argc, argv, &service, &network, &daemon, &status_frq);

    /* The TIB/Rendezvous machinery needs to be started. */
    return_code = tibrv_Open();
    if (return_code != TIBRV_OK)
    {
        fprintf(stderr,
                "%s failed to open the TIB/Rendezvous machinery: %s\n",
                program_name,
                tibrvStatus_GetText(return_code));
        exit(1);
    }

    /* Report version */
    tibrv_version = tibrv_Version();
    tibrvMsg_GetCurrentTimeString(localTime, gmtTime);
    fprintf(stdout,"%s: %s (TIBCO Rendezvous V%s C API)\n",
            gmtTime, program_name, tibrv_version);

    /* A transport needs to be created for server communication. */
    fprintf(stderr, "Create transport on service %s network %s daemon %s\n",
                        (service?service:"(default)"), (network?network:"(default)"),
                        (daemon?daemon:"(default)"));
    return_code = tibrvTransport_Create(&transport, service, network, daemon);
    if (return_code != TIBRV_OK)
    {
        fprintf(stderr,
                "%s failed to create search transport: %s\n",
                program_name,
                tibrvStatus_GetText(return_code));
        exit(1);
    }
    tibrvTransport_SetDescription(transport, program_name);

    /* This listener will pay attention to server searches. */
    return_code = tibrvEvent_CreateListener(&search_event,
                                            TIBRV_DEFAULT_QUEUE,
                                            searchCallback,
                                            transport,
                                            search_subject,
                                            NULL);
    if (return_code != TIBRV_OK)
    {
        fprintf(stderr,
                "%s failed to create a server search listener: %s\n",
                program_name,
                tibrvStatus_GetText(return_code));
        exit(1);
    }

     /* The server and client use point-to-point messaging for requests and
       responses.  If both use a transport eligible and enabled for direct
       communication, point-to-point messages will not go through a daemon. */
    tibrvTransport_CreateInbox(transport, inbox_subject, TIBRV_SUBJECT_MAX);

    /* Create a listener for messages with our request subject. */
    return_code = tibrvEvent_CreateListener(&request_event,
                                            TIBRV_DEFAULT_QUEUE,
                                            requestCallback,
                                            transport,
                                            inbox_subject,
                                            NULL);
    if (return_code != TIBRV_OK)
    {
        fprintf(stderr,
                "%s failed to create a client request listener: %s\n",
                program_name,
                tibrvStatus_GetText(return_code));
        exit(1);
    }

    /* We define the message we will use to reply to server searches.  This
       message will be reused if more than one search query is received. */
    return_code = tibrvMsg_Create(&search_reply);
    if (return_code != TIBRV_OK)
    {
        fprintf(stderr,
                "%s failed to create a reply to a server search: %s\n",
                program_name,
                tibrvStatus_GetText(return_code));
        exit(1);
    }

    /* Set the required request subject as the reply subject of our search
       reply.  The client will use it to send requests to the server. */
    return_code = tibrvMsg_SetReplySubject(search_reply, inbox_subject);
    if (return_code != TIBRV_OK)
    {
        fprintf(stderr,
                "%s failed to set reply subject for a server search reply: %s\n",
                program_name,
                tibrvStatus_GetText(return_code));
        exit(1);
    }

    /* Display a server-ready message. */
    tibrvMsg_GetCurrentTimeString(localTime, gmtTime);
    fprintf(stderr, "Listening for client searches on subject %s\n"
            "Listening for client requests on subject %s\n"
            "Wait time is %.0f secs\n%s: %s ready...\n",
            search_subject, inbox_subject, SERVER_TIMEOUT, gmtTime, program_name);


    /* If this server remains idle for more than the timeout value, exit. */
    while (1 == 1)
    {
        return_code = tibrvQueue_TimedDispatch(TIBRV_DEFAULT_QUEUE, SERVER_TIMEOUT);
        if (return_code != TIBRV_OK)
        {
            if (return_code != TIBRV_TIMEOUT)
                fprintf(stderr, "%s: TimedDispatch received status %x: %s\n",
                       program_name, return_code, tibrvStatus_GetText(return_code));
            else
                fprintf(stderr, "%s: TimedDispatch received timeout\n", program_name);
            break;
        }
    }

    tibrvMsg_GetCurrentTimeString(localTime, gmtTime);
    fprintf(stdout,"%s: %d client requests processed\n", gmtTime, requests);

    /* Destroy our Tibrv objects and close the Tibrv machinery. */
    tibrvMsg_Destroy(search_reply);
    tibrvEvent_Destroy(search_event);
    tibrvEvent_Destroy(request_event);
    tibrvTransport_Destroy(transport);
    tibrv_Close();

    exit(0);
}
