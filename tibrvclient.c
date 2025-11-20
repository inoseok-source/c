/*
 * Copyright (c) 1998-2023 Cloud Software Group, Inc.
 * All Rights Reserved.
 */

/*
 * TIB/Rendezvous client program
 *
 * This program will attempt to contact the server program and then
 * perform a series of tests;
 * This is NOT meant to measure the performance of RV
 * For performance measurements please use rvlat and rvperf
 *
 * This server example uses a transport enabled for direct communication
 * by default.  If the client also uses an enabled transport, and the
 * network path does not cross through RVRDs, the resulting requests and
 * replies will use direct communication instead of passing through
 * Rendezvous daemons.
 *
 * Optionally the user may specify transport parameters for the
 * communications used by the client application, and also the interval
 * between requests, and a status display frequency value.  If none are
 * specified, default values are used.  For information on standard
 * default values for the transport parameters, please see the TIBCO
 * Rendezvous Concepts manual.
 *
 * The following non-standard defaults are used in this sample program:
 *   service        "7522:7524"     service for client requests
 *   interval     0                 optional interval between client
 *                                  requests -- if non-zero, a timer is
 *                                  created and requests are sent from its
 *                                  callback
 *   status       0                 optional frequency of status display
 *                                  counts -- if non-zero, a message is
 *                                  printed every <n> messages sent or
 *                                  received.
 *   requests     10000             number of client requests to send
 *
 *
 * Examples:
 *
 *   Use service 7500, display status every 1000 messages for 20000
 *   client requests sent at intervals of .005 seconds:
 *     tibrvclient -service 7500 -status 1000 -interval 0.005 20000
 *
 *   Specify the loopback adapter to avoid sending to the subnet from a tight
 *   loop with no timer; use an an ephemeral port for direct communication
 *   (Note that two transport objects on a host cannot bind the same port
 *   simultaneously for direct communication.):
 *     tibrvclient -service 7522: -network 127.0.0.1
 *
 *   Specify a .01 second interval, status display every 1000 requests for
 *   20000 requests, and daemon host and port to prevent autostarting a daemon.
 *   If both client and server use this daemon value (with no other Rendezvous
 *   application which would restart the daemon) with direct-enabled transports,
 *   you can stop the daemon and observe that messages continue between the
 *   client and server with no daemon running.
 *     tibrvclient -daemon localhost:7500 -interval .01 -status 1000 50000
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "tibrv/tibrv.h"

#define DEFAULT_SERVICE     "7522:7524"     /* Two-part service parameter
                                               for direct communication.  To
                                               use ephemeral ports, specify
                                               in the form "7522:" */
#define DEFAULT_NETWORK     NULL
#define DEFAULT_DAEMON      NULL

#define DEFAULT_REQUESTS    10000
#define DEFAULT_INTERVAL    0.0             /* Default request interval (sec) */
#define DEFAULT_STATUS_FRQ  0               /* Default freq of status display */

#define SEARCH_SUBJECT      "TIBRV.LOCATE"  /* Clients use this subject
                                               to locate a server.       */

#define SEARCH_TIMEOUT      30.0            /* Clients quit searching for
                                               a server after SEARCH_TIMEOUT
                                               seconds have elapsed. */

#define REQUEST_TIMEOUT     10.0            /* Clients quit waiting for a
                                               reply from the server after
                                               REQUEST_TIMEOUT seconds have
                                               elapsed. */

#define WAIT_EXIT           5.0            /* If no data message has arrived
                                              has arrived from the server in
                                              WAIT_EXIT time, the the client
                                              assumes something is not OK,
                                              so it will exit */

#define DISP_TIMEOOUT       1.0            /* Main loop dispatch time out */

char*                       program_name;
static tibrvTransport       transport = TIBRV_INVALID_ID;
static tibrvQueue           response_queue = TIBRV_INVALID_ID;
static tibrvEvent           response_id = TIBRV_INVALID_ID;
static tibrvEvent           timer_id = TIBRV_INVALID_ID;
static tibrvEvent           timeout_id = TIBRV_INVALID_ID;
static tibrvDispatcher      response_thread = TIBRV_INVALID_ID;
static tibrvMsg             client_request = TIBRV_INVALID_ID;

static tibrv_u32            requests = DEFAULT_REQUESTS;
static tibrv_u32            responses = 0;
static tibrv_u32            sent = 0;
static tibrv_f64            interval = DEFAULT_INTERVAL;
static tibrv_u32            status_frq = DEFAULT_STATUS_FRQ;
static char                 request_subject[TIBRV_SUBJECT_MAX];
static char                 inbox_subject[TIBRV_SUBJECT_MAX];

char                        localTime[TIBRVMSG_DATETIME_STRING_SIZE];
char                        gmtTime[TIBRVMSG_DATETIME_STRING_SIZE];

static tibrv_bool           done = TIBRV_FALSE;
static tibrv_u32            last_response_count = 0;
static tibrv_status         tibrv_err = TIBRV_OK;

static int                 last_argument_index;
static char*               service = DEFAULT_SERVICE;
static char*               network = DEFAULT_NETWORK;
static char*               gdaemon  = DEFAULT_DAEMON;
static tibrvMsg            search_request = NULL;
static tibrvMsg            search_reply = NULL;
static const char*         search_subject = SEARCH_SUBJECT;
static const char*         server_subject = NULL;
static tibrvMsgDateTime    date_time_start, date_time_stop;
static tibrv_f64           time_start, time_stop, elapsed;
static const char*         tibrv_version;

/* This routine displays parameter information if invalid parameters are
   detected or the program is executed with a help request flag of
   -help or -h or -? */
void
usage(void)
{
    fprintf(stderr, "usage: tibrvclient  [-service <service>]\n");
    fprintf(stderr, "                    [-network  <network>]\n");
    fprintf(stderr, "                    [-daemon  <daemon>]\n");
    fprintf(stderr, "                    [-interval <publish interval in seconds>]\n");
    fprintf(stderr, "                    [-status <#msgs>]    <number of requests>\n");
    exit(1);
}


/* This routine parses the parameters on the command line. */
tibrv_u32
getParameters(int           argc,
              char*         argv[],
              char**        service,
              char**        network,
              char**        daemonp,
              tibrv_f64*    interval,
              tibrv_u32*    status_frq
              )
{
    char*     progptr;
    int       i = 1;

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
    else if (strrchr(program_name,']') != NULL)
    {   /* strip off directory information in / format */
        progptr = strrchr(program_name,']')+1;
        strncpy(program_name, progptr,
                (strlen(program_name)-(progptr-program_name)+1));
    }

    if (i < argc)
    {
        if ((strcmp(argv[i],"-h")==0) ||
            (strcmp(argv[i],"-help")==0) ||
            (strcmp(argv[i],"?")==0))
        {       /* if first command line argument is help flag,
                   display and quit. */
            usage();
        }
    }

    while (((i + 2) <= argc) && (*argv[i] == '-'))      /* These require a
                                                           value. */
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
            *daemonp = argv[i + 1];
            i += 2;
        }
        else if (strcmp(argv[i], "-interval") == 0)
        {
            *interval = (tibrv_f64) atof(argv[i+1]);
            i += 2;
        }
        else if (strcmp(argv[i], "-status") == 0)
        {
            *status_frq = (tibrv_u32) atol(argv[i+1]);
            i += 2;
        }
        else
        {
            usage();
        }
    }
    return i;
}


/* Put data into the request message. */
tibrv_status
set_msg_data(void)
{
    tibrv_u32           x;
    tibrv_u32           y;
    tibrv_status        return_code;

    /* Put some data into the message to the server. */
    x = ((tibrv_u32) rand());
    return_code = tibrvMsg_UpdateU32(client_request, "x", x);
    if (return_code != TIBRV_OK)
    {
        fprintf(stderr,
                "%s failed to update a client request: %s\n",
                program_name,
                tibrvStatus_GetText(return_code));
        tibrv_err = return_code;
    }
    else
    {
        y = ((tibrv_u32) rand());
        return_code = tibrvMsg_UpdateU32(client_request, "y", y);
        if (return_code != TIBRV_OK)
        {
            fprintf(stderr,
                    "%s failed to update a client request: %s\n",
                    program_name,
                    tibrvStatus_GetText(return_code));
            tibrv_err = return_code;
        }
    }

    return return_code;
}


/* This routine processes responses from our server when we send it messages
   after we have identified it and received its inbox address.  All we do
   here is count the replies, report the number received if indicated, and exit
   the callback if we have not received all the responses.  When all have
   been received, destroy the listener event. */
static void
serverResponse(
    tibrvEvent  event,
    tibrvMsg    msg,
    void*       arg)
{
    responses++;
    if (status_frq > 0)
    {
        if ((responses/status_frq)*status_frq == responses)
        {
            tibrvMsg_GetCurrentTimeString(localTime, gmtTime);
            fprintf(stdout,"%s: %d server responses received\n",
                    gmtTime, responses);
        }
    }

    if (responses >= requests)
    {
        tibrvMsg_GetCurrentTime(&date_time_stop);
        time_stop = date_time_stop.sec + (date_time_stop.nsec / 1000000000.0);
        elapsed = time_stop - time_start;
        /* We are done, so destroy our listener event and dispatcher thread. */
        tibrvEvent_Destroy(event);
        tibrvDispatcher_Destroy(response_thread);
        /* To avoid waiting for timeout in the TimedDispatch loop in main, we
            destroy the response queue here. */
        tibrvQueue_Destroy(response_queue);
        done = TIBRV_TRUE;
    }
    return;
}

/*
 * Time out after REQUEST_TIMEOUT from the time last message is sent to
 * the server
 * That is set done = TRUE, so that code breaks out from the dispatch loop.
 */
static void
timeOut(tibrvEvent      event,
       tibrvMsg        message,
       void *          closure)
{
    if (responses >= requests)
    {
        done = TIBRV_TRUE;
        tibrvEvent_Destroy(timeout_id);
    }
    else if (last_response_count == responses)
    {
        tibrvEvent_Destroy(timeout_id);
        tibrv_err = TIBRV_TIMEOUT;
    }
    else
    {
        last_response_count = responses;
    }
    return;
}

/*
 * Timer callback called according to specified interval.  Publishes a request
 * message each time the callback executes.
 */
static void
pubMsg(tibrvEvent      event,
       tibrvMsg        message,
       void *          closure)
{
    tibrv_status        return_code = TIBRV_OK;
    tibrvMsg            copyToSend;
    if (sent < requests)
    {
       /* Send a request message to the server. */
        return_code = set_msg_data();
        if (return_code == TIBRV_OK)
            return_code = tibrvMsg_CreateCopy(client_request, &copyToSend);
        if (return_code == TIBRV_OK)
            return_code = tibrvMsg_SetSendSubject(copyToSend, request_subject);
        if (return_code == TIBRV_OK)
            return_code = tibrvMsg_SetReplySubject(copyToSend, inbox_subject);
        if (return_code == TIBRV_OK)
            return_code = tibrvTransport_Send(transport, copyToSend);
        if (return_code != TIBRV_OK)
        {
            fprintf(stderr,
                    "%s failed to send a client request: %s\n",
                    program_name,
                    tibrvStatus_GetText(return_code));
        }
        else
        {
            /* Count it. */
            sent++;
            tibrvMsg_Destroy(copyToSend);
        }
    }

    if(return_code == TIBRV_OK)
    {
        /* Display status count if indicated by status frequency value. */
        if (status_frq > 0) {
            if (((sent)/status_frq)*status_frq == sent)
            {
                tibrvMsg_GetCurrentTimeString(localTime, gmtTime);
                fprintf(stdout, "%s: %d client requests sent\n",gmtTime, sent);
            }
        }
        if (sent >= requests)
        {
            /* Report the number of messages sent and number received
             * while sending.
             */
            fprintf(stdout,
                    "%d responses received while sending %d requests.\n",
                    responses, sent);
            /* We are done sending, so destroy the timer event. */
            tibrvEvent_Destroy(event);
        }
    }

    if (return_code != TIBRV_OK)
    {
        tibrv_err = return_code;
    }

    return;
}

tibrv_status
open_RVMechanisms( void )
{
    tibrv_status return_code = TIBRV_OK;
    /* The TIB/Rendezvous machinery needs to be started. */
    return_code = tibrv_Open();
    if (return_code != TIBRV_OK)
        fprintf(stderr,
                "%s failed to open the TIB/Rendezvous machinery: %s\n",
                program_name,
                tibrvStatus_GetText(return_code));
    return return_code;
}

tibrv_status
init_server_searching(void)
{
    tibrv_status return_code = TIBRV_OK;
    /* A transport needs to be 0d for the server communication. */
    fprintf(stdout, "Create a transport on service %s network %s daemon %s\n",
                        (service?service:"(default)"), (network?network:"(default)"),
                        (gdaemon?gdaemon:"(default)"));

    return_code = tibrvTransport_Create(&transport, service, network, gdaemon);


    /* We create the message we will send in order to locate a server. */
    if (return_code == TIBRV_OK)
        return_code = tibrvMsg_Create(&search_request);


    /* Set the send subject to locate our server. */
    if (return_code == TIBRV_OK)
        return_code = tibrvMsg_SetSendSubject(search_request, search_subject);

    return return_code;
}

tibrv_status
search_for_server(void)
{
    tibrv_status return_code = TIBRV_OK;

    fprintf(stdout,"%s is searching for a server on subject %s...\n",
                    program_name, search_subject);
    /* Send a request message to locate a server and receive its reply.
       SendRequest is a synchronous call which uses a private queue to
       receive its reply.  No external dispatching mechanism is involved. */
    return_code = tibrvTransport_SendRequest(transport,
                                             search_request,
                                             &search_reply,
                                             SEARCH_TIMEOUT);

    if (return_code != TIBRV_OK)
    {
        fprintf(stderr,
                "%s failed to locate a server: %s\n",
                program_name,
                tibrvStatus_GetText(return_code));
    }
    return return_code;

}

tibrv_status
get_server_data(void)
{
    tibrv_status return_code = TIBRV_OK;
    /*
       The search reply we receive from a server should contain a reply
       subject we can use to send requests to that server.
    */
    return_code = tibrvMsg_GetReplySubject(search_reply,
                       &server_subject);
    if (return_code == TIBRV_OK)
    {
        strcpy(request_subject, server_subject);
        fprintf(stdout,
                "%s successfully located a server: %s\n",
                program_name, request_subject);
        /* Destroy the server's reply message to reclaim memory. */
        return_code = tibrvMsg_Destroy(search_reply);
    }

    /* The server and client use point-to-point messaging for requests and
       responses.  If both use a transport eligible and enabled for direct
       communication, point-to-point messages will not go through a daemon. */
    if (return_code == TIBRV_OK)
        return_code = tibrvTransport_CreateInbox(transport, inbox_subject,
                                                 TIBRV_SUBJECT_MAX);
    return return_code;
}

tibrv_status
finish_setup(void)
{

    tibrv_status return_code = TIBRV_OK;
    /* Create response queue and a listener using the inbox subject for
       responses from the server to a series of messages. */
    return_code = tibrvQueue_Create(&response_queue);
    if (return_code == TIBRV_OK)
        return_code = tibrvEvent_CreateListener(&response_id,
                                                response_queue,
                                                serverResponse,
                                                transport,
                                                inbox_subject,
                                                NULL);
    if (return_code == TIBRV_OK)
    {
        return_code = tibrvDispatcher_CreateEx(&response_thread,
                                           response_queue,
                                           REQUEST_TIMEOUT);
    }
    /* Start a dispatcher thread to dispatch response messages. */
    if (return_code == TIBRV_OK)
        return_code = tibrvMsg_Create(&client_request);

    if (return_code == TIBRV_OK)
        return_code = set_msg_data();

    /* Set the send subject to the server's (inbox) subject. */
    if (return_code == TIBRV_OK)
        return_code = tibrvMsg_SetSendSubject(client_request,
                                            request_subject);

    /* Set the reply subject to our inbox subject, allowing a point-to-point
       reply from our server.  We won't use SendRequest, so we won't block
       waiting for th reply. */
    if (return_code == TIBRV_OK)
        return_code = tibrvMsg_SetReplySubject(client_request,
                                               inbox_subject);

    return return_code;
}

tibrv_status
send_msgs_cont(void)
{
    tibrv_status return_code = TIBRV_OK;
    tibrv_u32 i = 0;

    for (i = 0; i < requests; i++)
    {
        set_msg_data();
        /* Send a request message to the server. */
        return_code = tibrvTransport_Send(transport, client_request);
        if (return_code != TIBRV_OK)
        {
            fprintf(stderr,
                    "%s failed to send a client request: %s\n",
                    program_name,
                    tibrvStatus_GetText(return_code));
            break;
        }
        sent++;
        if (status_frq > 0) {
            if (((sent)/status_frq)*status_frq == sent) {
                    tibrvMsg_GetCurrentTimeString(localTime, gmtTime);
                    fprintf(stdout, "%s: %d client requests sent\n",
                            gmtTime, sent);
            }
        }
    }
    /* Report the number of messages sent and number received while sending. */
    fprintf(stdout,"%d responses received while sending %d requests.\n",
            responses, requests);

    return return_code;
}

void
report_results(void)
{

    if (responses >= requests)
    {
        fprintf(stdout, "%s received all %d server replies \n",
                    program_name, responses);
        fprintf(stdout, "%d requests took %.2f secs to process.\n",
                    requests, elapsed);
    }
    else
    {
        fprintf(stdout, "Received %d responses to  %d requests.\n",
                    responses, requests);
    }

}

/* This is the main routine. */
int
main(int argc, char** argv)
{
    tibrv_status        return_code = TIBRV_OK;
    last_argument_index = getParameters(argc, argv, &service,
                                        &network, &gdaemon,
                                        &interval, &status_frq);
    if (last_argument_index < argc)
    {
        requests = atoi(argv[last_argument_index]);
    }
    return_code = open_RVMechanisms();
    if (return_code == TIBRV_OK)
    {
       /* Report Rendezvous version */
        tibrv_version = tibrv_Version();
        tibrvMsg_GetCurrentTimeString(localTime, gmtTime);
        fprintf(stdout,"%s: %s (TIBCO Rendezvous V%s C API)\n",
                gmtTime, program_name, tibrv_version);
        return_code = init_server_searching();
    }
    if (return_code == TIBRV_OK)
    {
        return_code = search_for_server();
    }
    if (return_code == TIBRV_OK)
    {
        return_code = get_server_data();
    }
    if (return_code == TIBRV_OK)
    {
        return_code = finish_setup();
    }
    if (return_code == TIBRV_OK)
    {
        tibrvMsg_GetCurrentTimeString(localTime, gmtTime);
        fprintf(stdout, "%s: Starting test...\n", gmtTime);
        /* We will time this test. */
        tibrvMsg_GetCurrentTime(&date_time_start);
        time_start = date_time_start.sec + (date_time_start.nsec / 1000000000.0);
        last_response_count = responses;
        return_code = tibrvEvent_CreateTimer(
                                 &timeout_id,
                                 response_queue,
                                 timeOut,
                                 (WAIT_EXIT + interval),
                                 "");
        if (return_code != TIBRV_OK)
        {
            fprintf(stderr,"Error adding the timeout event: --%s\n",
                    tibrvStatus_GetText(return_code));
        }
    }
    if (return_code == TIBRV_OK)
    {
        if (interval == 0.0)
        {
            return_code = send_msgs_cont();
        }
        else
        {
            return_code = tibrvEvent_CreateTimer(&timer_id,
                                                 TIBRV_DEFAULT_QUEUE,
                                                 pubMsg,
                                                 interval,
                                                 "");
        }
    }

    while(!done && (return_code == TIBRV_OK))
    {
        if (responses < requests)
        {
            tibrvQueue_TimedDispatch(TIBRV_DEFAULT_QUEUE,
                                                    DISP_TIMEOOUT);
            if (tibrv_err != TIBRV_OK)
                return_code = tibrv_err;
        }
        else
            break;
    }

    if (return_code != TIBRV_OK)
    {
        fprintf(stderr,"Failed with error: --%s\n",
                tibrvStatus_GetText(return_code));
    }
    else
    {
        report_results();
    }

    if (search_request != TIBRV_INVALID_ID)
        tibrvMsg_Destroy(search_request);
    if (client_request != TIBRV_INVALID_ID)
        tibrvMsg_Destroy(client_request);
    if (response_id != TIBRV_INVALID_ID)
        tibrvEvent_Destroy(response_id);
    if (timeout_id != TIBRV_INVALID_ID)
        tibrvQueue_Destroy(timeout_id);
    if (response_thread != TIBRV_INVALID_ID)
        tibrvDispatcher_Destroy(response_thread);
    if (transport != TIBRV_INVALID_ID)
        tibrvTransport_Destroy(transport);
    /* Close the Tibrv machinery and exit. */
    tibrv_Close();
    return return_code;
}
