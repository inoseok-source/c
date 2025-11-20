/*
 * Copyright (c) 1998-2023 Cloud Software Group, Inc.
 * All Rights Reserved.
 */

/*
 * TIB/Rendezvous Virtual Circuit client Program
 *
 * This program will attempt to contact the server program and then
 * perform a seried of tests to determine msg throughput and response
 * times.
 *
 * Optionally the user may specify transport parameters for the
 * communications used by the client application, and also the interval
 * between requests, and a status display frequency value.  If none are
 * specified, default values are used.  For information on standard
 * default values for the transport parameters, please see the TIBCO
 * Rendezvous Concepts manual.
 *
 * The following defaults are used in this sample program:
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
 * Example:
 *
 * Set up a vc client using service 7725, the loopback adapter, a daemon
 * listening on 7505, and sending 20000 requests.
 *  tibrvvcclient -service 7725 -network 127.0.0.1 -daemon 7505 20000
 *
 * Use an ephemeral port for direct communication, and specify a daemon host
 * and port to prevent autostarting a daemon, with a .01 second interval, and
 * status display every 1000 requests for 20000 requests,  If both client and
 * server use this daemon value (with no other Rendezvous application which
 * would restart the daemon) with direct-enabled transports, you can stop the
 * daemon and observe that messages continue between the client and server
 * with no daemon running.
 *  tibrvvcclient -service 7522: -daemon localhost:7500 -interval .01 -status 1000 50000
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "tibrv/tibrv.h"

#define DEFAULT_SERVICE     NULL
#define DEFAULT_NETWORK     NULL
#define DEFAULT_DAEMON      NULL

#define DEFAULT_REQUESTS    10000
#define DEFAULT_STATUS_FRQ  0
#define DEFAULT_INTERVAL    0               /* Default request interval (sec) */

#define CONNECTED_ADVISORY  "_RV.INFO.SYSTEM.VC.CONNECTED"
#define DISCONNECTED_ADVISORY   "_RV.*.SYSTEM.VC.DISCONNECTED"


#define SEARCH_SUBJECT      "TIBRV.VC.LOCATE"  /* Clients use this subject
                                               to locate a VC server.       */

#define SEARCH_TIMEOUT      60.0            /* Clients quit searching for
                                               a server after SEARCH_TIMEOUT
                                               seconds have elapsed. */

#define VC_CONNECT_TIMEOUT  20.0            /*  VC Connection Timeout */

#define REQUEST_TIMEOUT     10.0            /* Clients quit waiting for a
                                               reply from the server after
                                               REQUEST_TIMEOUT seconds have
                                               elapsed. */

#define REQUEST_SUBJECT     "TIBRV.VC.WORK" /* Clients address messages using
                                               this subject and send them on the
                                               virtual circuit.  Even if another
                                               application subscribes to this subject
                                               it will not receive it because it has
                                               been sent point to point on the VC. */

#define RESPONSE_SUBJECT    "TIBRV.VC.REPLY" /* a non-inbox reply subject*/

char*                       program_name;

/*
 * This is in case we want to know the state of a VC within the client
 * in the future.  These are arbitrary and reflect the design of this
 * example and not any internal state of the VC.
 */
typedef enum  {
    initializing=0,
    waiting,
    connected,
    disconnected
} vcState;

/*
 *  A bookkeeping structure for our virtual circuits.  One of these
 *  is created for each VC that we setup.
 */
typedef struct vcStruct {
    vcState         vc_state;           /* our own state of the VC */
    unsigned int    msgs_in;
    unsigned int    msgs_out;
    tibrvEvent      vc_disconnect_event;
    tibrvEvent      vc_msg_event;
    tibrvTransport  vc_transport;       /* the VC transport we create */
    char            vc_inbox[TIBRV_SUBJECT_MAX];
    char            localTime[TIBRVMSG_DATETIME_STRING_SIZE];
    char            gmtTime[TIBRVMSG_DATETIME_STRING_SIZE];
} vcRec, *vcPtr;

static tibrvDispatcher  response_thread;
static tibrvQueue       wait_queue;
static tibrvMsg         client_request;
static tibrvEvent       timer_event;

static tibrv_u32        requests = DEFAULT_REQUESTS;
static unsigned int     status_frq = DEFAULT_STATUS_FRQ;
static tibrv_f64        interval = DEFAULT_INTERVAL;

static int              exit_status = 1;

static char             request_subject[TIBRV_SUBJECT_MAX] = REQUEST_SUBJECT;
static char             response_subject[TIBRV_SUBJECT_MAX] = RESPONSE_SUBJECT;



/* This routine displays parameter information if invalid parameters are
   detected or the program is executed with a help request flag of
   -help or -h or -? */
void
usage(void)
{
    fprintf(stderr,"tibrvvcclient  [-service service] [-network network]\n");
    fprintf(stderr,"               [-daemon daemon]   [-interval <secs>]\n");
    fprintf(stderr,"               [-status <#msgs>]  <number of requests>\n");
    exit(1);
}



/* This routine parses the parameters on the command line. */
tibrv_u32
getParameters(int           argc,
              char*         argv[],
              char**        service,
              char**        network,
              char**        daemon,
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
            *daemon = argv[i + 1];
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
void
set_msg_data(void)
{
    tibrv_u32           x;
    tibrv_u32           y;
    tibrv_u32           sum;
    tibrv_status        return_code;

    /* Put some data into the message to the server. */
    x = ((tibrv_u32) rand());
    return_code = tibrvMsg_UpdateU32(client_request, "x", x);
    if (return_code != TIBRV_OK)
    {
        fprintf(stderr, "%s failed to update a client request: %s\n",
                program_name, tibrvStatus_GetText(return_code));
        exit(1);
    }

    y = ((tibrv_u32) rand());
    return_code = tibrvMsg_UpdateU32(client_request, "y", y);
    if (return_code != TIBRV_OK)
    {
        fprintf(stderr, "%s failed to update a client request: %s\n",
                program_name, tibrvStatus_GetText(return_code));
        exit(1);
    }

    sum = x + y;
    return_code = tibrvMsg_UpdateU32(client_request, "sum", sum);
    if (return_code != TIBRV_OK)
    {
        fprintf(stderr, "%s failed to update a client request: %s\n",
                program_name, tibrvStatus_GetText(return_code));
        exit(1);
    }
    return;
}


/*
 * This routine creates the vc transport and waits for the server to
 * accept it.  */
tibrv_status
createServerConnection(
    tibrvTransport  transport,
    char*           search_subject,
    tibrv_f64       search_timeout,
    tibrvTransport* vc_transport)
{
    const char*     connect_subject = NULL;

    tibrv_status    return_code;
    tibrvMsg        search_request, search_reply;

    /* We create the message we will send in order to locate a server. */
    return_code = tibrvMsg_Create(&search_request);
    if (return_code != TIBRV_OK)
    {
        fprintf(stderr, "%s failed to create a search request: %s\n",
                program_name, tibrvStatus_GetText(return_code));
        return(return_code);
    }

    /* Set the send subject to locate our server. */
    return_code = tibrvMsg_SetSendSubject(search_request,
                                          search_subject);
    if (return_code != TIBRV_OK)
    {
        fprintf(stderr,
                "%s failed to set the send subject of a search request: %s\n",
                program_name, tibrvStatus_GetText(return_code));
        return(return_code);
    }

    fprintf(stdout, "%s is searching for a server on subject %s...\n",
            program_name, SEARCH_SUBJECT);

    /* Send a request message to locate a server and receive its reply.
       SendRequest is a synchronous call which uses a private queue to
       receive its reply.  No external dispatching mechanism is involved. */

    return_code = tibrvTransport_SendRequest(transport,
                                             search_request,
                                             &search_reply,
                                             search_timeout);
    if (return_code != TIBRV_OK)
    {
        fprintf(stderr, "%s failed to locate a server: %s\n",
                program_name, tibrvStatus_GetText(return_code));
        return(return_code);
    }

    /*
      The search reply we receive from a server should contain a vc
      connect subject we can use to setup our vc to the server.
    */

    return_code = tibrvMsg_GetReplySubject(search_reply,
                       &connect_subject);
    if (return_code != TIBRV_OK)
    {
        fprintf(stderr,
                "%s failed to get the connect_subject out of a search_reply: %s\n",
                program_name, tibrvStatus_GetText(return_code));
        return(return_code);
    }

    fprintf(stdout, "%s successfully located a server: %s\n",
            program_name, request_subject);

    /* connect virtual circuit to server and obtain vc transport */
    return_code = tibrvTransport_CreateConnectVc(vc_transport, connect_subject,
        transport);
    return_code = tibrvTransport_WaitForVcConnection(*vc_transport,
                            VC_CONNECT_TIMEOUT);

    if (return_code != TIBRV_OK)
    {
        fprintf(stderr,
                "%s failed to create VC connection to server\n - %s\n",
                program_name, tibrvStatus_GetText(return_code));
        return(return_code);
    }


    tibrvTransport_SetDescription(*vc_transport, program_name);


    /* Destroy the server's reply message to reclaim memory. */
    return_code = tibrvMsg_Destroy(search_reply);
    if (return_code != TIBRV_OK)
    {
        fprintf(stderr,
                "%s failed to destroy a reply to a client request: %s\n",
                program_name, tibrvStatus_GetText(return_code));
        return(return_code);
    }
    return(TIBRV_OK);
}



/*
 *  this routine gets called when we get notification that the
 *  test is completed via the TEST.COMPLETED message.
 */
void
testcompletedCB(
    tibrvEvent  event,
    tibrvMsg    msg,
    void*       arg)
{
    fprintf(stderr, "%s: test completion message received.\n",
            program_name);
    return;
}



/* This routine forces past our waiting point. */
static void
allDone(void)
{
    /* Send a message to myself wake up the main thread
    that is waiting.  When it wakes up, we will pass
    through the the cleanup and shutdown of the process */

    tibrvMsg_SetSendSubject(client_request,
                                        "TEST.COMPLETED");
    tibrvTransport_Send(TIBRV_PROCESS_TRANSPORT,
                                        client_request);
    return;
}


/*
 *  This callback is executed whenever we get a virtual circuit disconnect
 *  advisory.  It unwinds all of the virtual circuit setup logic that we
 *  execute in response to a search request (in searchCallback()).
 */
static void
disconnectedVcCallback(tibrvEvent event,
                tibrvMsg    message,
                void*       closure)
{
    /* tibrv_status    return_code; */
    vcRec*          vc = (vcRec*)closure;
    const char*     msgString;

    tibrvMsg_GetCurrentTimeString(vc->localTime, vc->gmtTime);
    tibrvMsg_ConvertToString(message, &msgString);
    fprintf(stderr, "%s: virtual circuit has been disconnected.\n%s\n",
            program_name, msgString);
    fprintf(stderr,"%s: %d requests sent, %d responses received\n",
            vc->gmtTime, vc->msgs_out, vc->msgs_in);

    vc->vc_state = disconnected;

    /* send the completion message, since disconnection prevents any
    more requests or responses. */
    allDone();
    return;
}


/* This routine processes responses from our server when we send it messages
   after we have identified it and received its subject for client requests.
   All we do here is count the replies and exit if we have not received all
   the responses.

   When all have been received, destroy the queue to end all dispatching.
 */
static void
serverResponse(
    tibrvEvent  event,
    tibrvMsg    msg,
    void*       closure)
{
    vcRec*      vc = (vcRec*)closure;

    vc->msgs_in++;
    /* Report message count every <status_frq> messages received, after all our requests
     * have been sent.  While sending, we report every <status_frq> messages sent.*/
    if (vc->msgs_out == requests && status_frq > 0) {
        if ((vc->msgs_in/status_frq)*status_frq == vc->msgs_in) {
            tibrvMsg_GetCurrentTimeString(vc->localTime, vc->gmtTime);
            fprintf(stderr,"%s: %d requests sent, %d responses received\n",
                    vc->gmtTime, vc->msgs_out, vc->msgs_in);
        }
    }
    if (vc->msgs_in >= requests)
    {
        allDone();
    }
}



/*
 * Timer callback called according to specified interval.  Publishes a request
 * message each time the callback executes.
 */
static void
pubReqMsg(tibrvEvent    event,
       tibrvMsg         message,
       void *           closure)
{
    vcRec*              vc = (vcRec*)closure;
    tibrv_status        return_code;

    if (vc->msgs_out < requests)
    {
       /* Send a request message to the server. */
        set_msg_data();
        return_code = tibrvTransport_Send(vc->vc_transport,
                                          client_request);
        if (return_code != TIBRV_OK)
        {
            fprintf(stderr, "%s failed to send a client request: %s\n",
                    program_name, tibrvStatus_GetText(return_code));
            exit(1);
        }
        /* Count it. */
        vc->msgs_out++;
    }

    /* Display status count if indicated by status frequency value. */
    if (status_frq > 0) {
        if (((vc->msgs_out)/status_frq)*status_frq == vc->msgs_out)
        {
            tibrvMsg_GetCurrentTimeString(vc->localTime, vc->gmtTime);
            fprintf(stdout, "%s: %d requests sent, %d responses received\n",
                    vc->gmtTime, vc->msgs_out, vc->msgs_in);
        }
    }

    if (vc->msgs_out == requests)
    {
        /* Report the number of messages sent and number received while sending. */
        fprintf(stdout,
            "%d responses received while sending %d requests.\n",
            vc->msgs_in, vc->msgs_out);
        /* We are done sending, so destroy the timer event. */
        return_code = tibrvEvent_Destroy(event);
        if (return_code != TIBRV_OK)
        {
            fprintf(stderr, "%s failed to destroy timer event %d: %s.\n",
                    program_name, timer_event,
                    tibrvStatus_GetText(return_code));
        }
    }

    return;
}



/* This is the main routine. */
int
main(int argc, char** argv)
{
    int                 last_argument_index;

    tibrv_status        return_code;

    char*               service = DEFAULT_SERVICE;
    char*               network = DEFAULT_NETWORK;
    char*               daemon  = DEFAULT_DAEMON;

    vcRec*              vc;

    tibrvTransport      transport;
    tibrvEvent          wait_event;

    tibrvMsgDateTime    date_time_start, date_time_stop;
    tibrv_f64           time_start, time_stop, elapsed;

    tibrv_u32           i;
    char                localTime[TIBRVMSG_DATETIME_STRING_SIZE];
    char                gmtTime[TIBRVMSG_DATETIME_STRING_SIZE];
    const char*         tibrv_version;

    char*       search_subject = SEARCH_SUBJECT;
    tibrv_f64   search_timeout = SEARCH_TIMEOUT;

    /* Parse the command line. */
    last_argument_index = getParameters(argc, argv, &service, &network, &daemon,
                                        &interval, &status_frq);
    if (last_argument_index < argc) {
        requests = atoi(argv[last_argument_index]);
    }

    /* The TIB/Rendezvous machinery needs to be started. */
    return_code = tibrv_Open();
    if (return_code != TIBRV_OK)
    {
        fprintf(stderr,
                "%s failed to open the TIB/Rendezvous machinery: %s\n",
                program_name, tibrvStatus_GetText(return_code));
        exit(1);
    }

    /* Report Rendezvous version */
    tibrv_version = tibrv_Version();
    tibrvMsg_GetCurrentTimeString(localTime, gmtTime);
    fprintf(stdout,"%s: %s (TIBCO Rendezvous V%s C API)\n",
            gmtTime, program_name, tibrv_version);

    /* A network transport needs to be created. */
    return_code = tibrvTransport_Create(&transport, service, network, daemon);
    if (return_code != TIBRV_OK)
    {
        fprintf(stderr, "%s failed to create a transport: %s\n",
                program_name, tibrvStatus_GetText(return_code));
        exit(1);
    }
    tibrvTransport_SetDescription(transport, program_name);

    /* locate a server and establish a virtual circuit */

    /* first step: create and populate our VC bookkeeping structure */
    vc = (vcRec*)malloc(sizeof(vcRec));
    vc->vc_state = initializing;
    vc->msgs_in = 0;
    vc->msgs_out = 0;

    /* next: create a vc connection.  Exit if the connection is not
     * accepted by a server. */
    return_code = createServerConnection(transport,
                            search_subject, search_timeout,
                            &vc->vc_transport);

    if (return_code != TIBRV_OK)
    {
        fprintf(stderr, "%s unable to connect to server\n - %s\n",
                program_name, tibrvStatus_GetText(return_code));

        exit(1);
    }

    /*
     *  Create a listener responsible for responses from the
     *  server to our queries.
     */
    return_code = tibrvEvent_CreateListener(&vc->vc_msg_event,
                                            TIBRV_DEFAULT_QUEUE,
                                            serverResponse,
                                            vc->vc_transport,
                                            response_subject,
                                            (void*)vc);
    if (return_code != TIBRV_OK)
    {
        fprintf(stderr, "%s failed to initialize response mechanism: %s\n",
                program_name, tibrvStatus_GetText(return_code));
        exit(1);
    }

    /* create a queue to wait on until we have received the
    event that tells us that we are finished */
    return_code = tibrvQueue_Create(&wait_queue);
    return_code = tibrvEvent_CreateListener(&wait_event,
                                            wait_queue,
                                            testcompletedCB,
                                            TIBRV_PROCESS_TRANSPORT,
                                            "TEST.COMPLETED",
                                            NULL);
    if (return_code != TIBRV_OK)
    {
        fprintf(stderr, "%s failed to initialize shutdown mechanism: %s\n",
                program_name, tibrvStatus_GetText(return_code));
        exit(1);
    }


    /* Register disconnect callback to check for loss of the
     * virtual circuit.  If the circuit is disconnected, we will be
     * unable to continue, so we will process this on the wait queue.  */

    return_code = tibrvEvent_CreateListener(&vc->vc_disconnect_event,
                                            wait_queue,
                                            disconnectedVcCallback,
                                            vc->vc_transport,
                                            DISCONNECTED_ADVISORY,
                                            (void*)vc);

    if (return_code != TIBRV_OK)
    {
        fprintf(stderr,
                "%s: failed to set the DISCONNECT_ADVISORY callback: %s\n",
                program_name, tibrvStatus_GetText(return_code));
        exit(1);
    }


    /*
     *  Start a dispatcher thread to dispatch responses while the
     *  main thread is still sending.  No point in waiting.
     */
    return_code = tibrvDispatcher_Create(&response_thread, TIBRV_DEFAULT_QUEUE);
    if (return_code != TIBRV_OK)
    {
        fprintf(stderr, "failed to create response dispatcher--%s\n",
                tibrvStatus_GetText(return_code));
        exit(1);
    }

    /* Because this is only a test, we will repeatedly send the same request
       message to the server.  We'll change the data in the message each time.
    */

    return_code = tibrvMsg_Create(&client_request);
    if (return_code != TIBRV_OK)
    {
        fprintf(stderr, "%s failed to create a client request: %s\n",
                program_name, tibrvStatus_GetText(return_code));
        exit(1);
    }

    /* Set the send subject to the server's work subject. */
    return_code = tibrvMsg_SetSendSubject(client_request,
                                          request_subject);
    if (return_code != TIBRV_OK)
    {
        fprintf(stderr,
                "%s failed to set the send subject of a client request: %s\n",
                program_name, tibrvStatus_GetText(return_code));
        exit(1);
    }

    /* Set the reply subject to response subject.  This is not an inbox
       subject.  Because we use a virtual circuit, the server's responses
       will be point to point.  We won't use SendRequest, so we won't block
       waiting for the reply. */
    return_code = tibrvMsg_SetReplySubject(client_request,
                                           response_subject);
    if (return_code != TIBRV_OK)
    {
        fprintf(stderr,
                "%s failed to set the reply subject of a client request: %s\n",
                program_name, tibrvStatus_GetText(return_code));
        exit(1);
    }

    fprintf(stdout, "Starting test...\n");

    /* We will time this test. */
    tibrvMsg_GetCurrentTime(&date_time_start);
    time_start = date_time_start.sec + (date_time_start.nsec / 1000000000.0);

    if (interval == 0)
    {
        for (i = 0; i < requests; i++)
        {
            /* Send a request message to the server. */
            set_msg_data();
            return_code = tibrvTransport_Send(vc->vc_transport,
                                              client_request);
            if (return_code != TIBRV_OK)
            {
                fprintf(stderr, "%s failed to send client request %d: %s\n",
                        program_name, (vc->msgs_out+1),
                        tibrvStatus_GetText(return_code));
                exit(1);
            }
            vc->msgs_out++;

            if (status_frq > 0) {
                if ((vc->msgs_out/status_frq)*status_frq == vc->msgs_out) {
                    tibrvMsg_GetCurrentTimeString(localTime, gmtTime);
                    fprintf(stderr,
                            "%s: %d requests sent, %d responses received\n",
                            gmtTime, vc->msgs_out, vc->msgs_in);
                }
            }
        }

        /* Report the number of messages sent and number received while sending. */
        fprintf(stdout,
                "%d request messages sent.  %d messages received while sending.\n",
                vc->msgs_out, vc->msgs_in);
    } else {
        /* Create a timer.  We will send a message each time we execute the callback
           until all the requests have been sent. */
        return_code = tibrvEvent_CreateTimer(&timer_event,
                                             TIBRV_DEFAULT_QUEUE,
                                             pubReqMsg,
                                             interval,
                                             (void*)vc);
        if(return_code != TIBRV_OK)
        {
            fprintf(stderr, "Error adding repeating timer: --%s\n",
                    tibrvStatus_GetText(return_code));
            exit(1);
        }
    }

    /* wait here until we're finished dispatching in the other thread */
    return_code = tibrvQueue_Dispatch(wait_queue);

    /* Report the run statistics for this test. */
    tibrvMsg_GetCurrentTime(&date_time_stop);
    time_stop = date_time_stop.sec + (date_time_stop.nsec / 1000000000.0);

    elapsed = time_stop - time_start;

    if (vc->msgs_in >= requests)
    {
        fprintf(stdout, "%s received all %d server replies\n",
                program_name, vc->msgs_in);
        fprintf(stdout, "%d requests took %.2f secs to process.\n",
                requests, elapsed);
        fprintf(stdout,
                "This result implies an effective rate of %.1f requests/second.\n",
                ((tibrv_f64) requests / elapsed));
        exit_status = 0;
    }
    else
    {
        fprintf(stdout, "Received %d responses to %d requests.\n",
                vc->msgs_in, vc->msgs_out);
        exit_status = 1;
    }

    /*Destroy our response listener */
    tibrvEvent_Destroy(vc->vc_msg_event);

    /* destroy our disconnect advisory listener. */
    tibrvEvent_Destroy(vc->vc_disconnect_event);

    /* destroy our vc transport. */
    tibrvTransport_Destroy(vc->vc_transport);

    /* Destroy our dedicated dispatcher thread. */
    tibrvDispatcher_Destroy(response_thread);

    /* Destroy our wait queue. */
    tibrvQueue_Destroy(wait_queue);

    /* Destroy our network transport. */
    tibrvTransport_Destroy(transport);

    /* Close the Tibrv machinery and exit. */
    tibrv_Close();

    exit(exit_status);
}
