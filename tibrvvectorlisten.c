/*
 * Copyright (c) 1998-2023 Cloud Software Group, Inc.
 * All Rights Reserved.
 */

/*
 * tibrvvectorlisten.c
 *
 * This program and tibrvvectorlistenertester demonstrate the behavior of the
 * vector listener.  As such it is not an example of the best way to write such
 * applications.
 *
 * The corresponding receiver (tibrvvectorlisten) does the following:
 *  Create vector listener 1 on subject "a.>" with vectorcallback_1.
 *  Create vector listener 2 on subject "b.>" with vectorcallback_1.
 *  Create vector listener 3 on subject "c.>" with vectorcallback_2.
 *  Create simple listener   on subject "a.1" with simpleCallback.
 *
 * Messages come in from TibrvVectorListenerTester (this program) in the following order:
 *      a.2, a.3, b.1, b.2, b.3, a.1, a.4, b.4, c.1  repeated 10 times
 *
 * The callbacks are driven as follows:
 *
 * vectorcallback_1 with a vector of  a.2, a.3, b.1, b.2, b.3, a1
 *      (possible in a sigle invocation)
 *
 * simplecallback with a.1
 *
 * vectorcallback_1 with a vector of -  a.4, b.4 (possible in a sigle invocation)
 *
 * vectorcallback_2 with a vector of c.1
 *
 * This illustrates that it is very likely that vectorcallback_1 would get
 * vectors containing messages combined from the first two listeners.
 *
 * We added a second queue "waitQueue" that dispatches nothing but it is used to
 * wait 1.0 sec after each dispatch on default queue, giving time for more
 * messages to arrive so we can capture the behavior described above.
 */

#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <time.h>

#include "tibrv/tibrv.h"

typedef struct
{
    char listenerName[24];

} closureRec, *closurePtr;

void
simplecallback(
    tibrvEvent          event,
    tibrvMsg            message,
    void*               closure)
{
    const char*         send_subject  = NULL;
    tibrvEvent          id;
    closurePtr          closureFromMsg = NULL;
    printf("Simplecallback: called with the following message\n");
    /*
     * Get the subject name to which this message was sent.
     */
    tibrvMsg_GetSendSubject(message, &send_subject);
    tibrvMsg_GetClosure(message,(void*)&closureFromMsg);
    tibrvMsg_GetEvent(message,&id);


    printf("\t subject=%s, listener name = %s listener id = %d\n",
                send_subject, closureFromMsg->listenerName,id);
    printf("\t Listener  passed to simplecallback function %d ; listener extracted from message %d\n", event,id);
    printf("\t Closure passed to simplecallback function %s   ;  closure extracted from message %s\n",
             closureFromMsg->listenerName,((closurePtr)closure)->listenerName);

    fflush(stdout);
}

void
vectorcallback_2(
    tibrvMsg            messages[],
    tibrv_u32           numMessages)
{
    const char*         send_subject  = NULL;
    tibrv_u32           i = 0;
    tibrvEvent          id;
    closurePtr          closureFromMsg = NULL;

    printf("Vectorcallback_2: called with  %d messages  and they have the subjects\n",numMessages);
    for(i=0;i<numMessages;i++)
    {
        /*
        * Get the subject name to which this message was sent.
        */
        tibrvMsg_GetSendSubject(messages[i], &send_subject);

        tibrvMsg_GetClosure(messages[i],(void*)&closureFromMsg);
        tibrvMsg_GetEvent(messages[i],&id);

        printf("\t subject=%s ; listener name = %s ; listener id = %d\n",
            send_subject, closureFromMsg->listenerName,id);

        fflush(stdout);
    }
}

void
vectorcallback_1(
    tibrvMsg            messages[],
    tibrv_u32           numMessages)
{
    const char*         send_subject  = NULL;
    tibrv_u32           i = 0;
    tibrvEvent          id;
    closurePtr          closureFromMsg = NULL;

    printf("Vectorcallback_1: called with  %d messages  and they have the subjects\n",numMessages);
    for(i=0;i<numMessages;i++)
    {
        /*
            * Get the subject name to which this message was sent.
            */
        tibrvMsg_GetSendSubject(messages[i], &send_subject);

        tibrvMsg_GetClosure(messages[i],(void*)&closureFromMsg);
        tibrvMsg_GetEvent(messages[i],&id);

        printf("\t subject=%s ; listener name = %s ; listener id = %d\n",
                send_subject, closureFromMsg->listenerName,id);


        fflush(stdout);
    }
}

void
usage(void)
{
    fprintf(stderr,"tibrvvectorlisten [-service service] [-network network] \n");
    fprintf(stderr,"            [-daemon daemon]\n");
    exit(1);
}

/*********************************************************************/
/* get_InitParms:  Get from the command line the parameters that can */
/*                 be passed to tibrvTransport_Create().             */
/*                                                                   */
/*                 returns the index for where any additional        */
/*                 parameters can be found.                          */
/*********************************************************************/
int
get_InitParms(
    int         argc,
    char*       argv[],
    char**      serviceStr,
    char**      networkStr,
    char**      daemonStr)
{
    int i = 1;


    for (i = 1; i < argc; i++)
    {
        if(strcmp(argv[i], "-service") == 0)
        {
            if (++i >= argc)
                usage();
            *serviceStr = argv[i];
        }
        else if(strcmp(argv[i], "-network") == 0)
        {
            if (++i >= argc)
                usage();
            *networkStr = argv[i];
        }
        else if(strcmp(argv[i], "-daemon") == 0)
        {
            if (++i >= argc)
                usage();
            *daemonStr = argv[i];
        }
        else
        {
            usage();
        }
    }

    return( i );
}

int
main(int argc, char** argv)
{
    tibrv_status        err;
    tibrvEvent          vectorListenerId_1;
    tibrvEvent          vectorListenerId_2;
    tibrvEvent          vectorListenerId_3;
    tibrvEvent          simpleListenerId;

    tibrvTransport      transport;
    tibrvQueue          waitQueue;
    char*               serviceStr = NULL;
    char*               networkStr = NULL;
    char*               daemonStr  = NULL;

    char*               progname = argv[0];

    /*
     * allocate  closure for  listeners that we're going to create later
     */
    closurePtr          closure_1 = (closurePtr)malloc(sizeof(closureRec));
    closurePtr          closure_2 = (closurePtr)malloc(sizeof(closureRec));
    closurePtr          closure_3 = (closurePtr)malloc(sizeof(closureRec));
    closurePtr          closure_4 = (closurePtr)malloc(sizeof(closureRec));
    if(closure_1 != NULL)
    {
        strncpy(closure_1->listenerName,"Vectored Listener 1",24);
    }
    else
    {
        fprintf(stderr, "%s: Failed to allocate memory for  closure struc \n",
                progname);
        exit(1);
    }

    if(closure_2 != NULL)
    {
        strncpy(closure_2->listenerName,"Vectored Listener 2",24);
    }
    else
    {
        fprintf(stderr, "%s: Failed to allocate memory for  closure struc \n",
                progname);
        exit(1);
    }

    if(closure_3 != NULL)
    {
        strncpy(closure_3->listenerName,"Vectored Listener 3",24);
    }
    else
    {
        fprintf(stderr, "%s: Failed to allocate memory for  closure struc \n",
                progname);
        exit(1);
    }

    if(closure_4 != NULL)
    {
        strncpy(closure_4->listenerName,"Listener 4",24);
    }
    else
    {
        fprintf(stderr, "%s: Failed to allocate memory for  closure struc \n",
                        progname);
        exit(1);
    }

    /*
     * Parse the arguments for possible optional parameter pairs.
     * These must precede the subject and message strings.
     */

    get_InitParms(argc, argv, &serviceStr, &networkStr, &daemonStr);

    /* Create internal TIB/Rendezvous machinery */
    err = tibrv_Open();
    if (err != TIBRV_OK)
    {
        fprintf(stderr, "%s: Failed to open TIB/Rendezvous: %s\n",
                progname, tibrvStatus_GetText(err));
        exit(1);
    }


    /*
     * Initialize the transport with the given parameters or default NULLs.
     */

    err = tibrvTransport_Create(&transport, serviceStr,
                                networkStr, daemonStr);
    if (err != TIBRV_OK)
    {
        fprintf(stderr, "%s: Failed to initialize transport: %s\n",
                progname, tibrvStatus_GetText(err));
        exit(1);
    }

    tibrvTransport_SetDescription(transport, progname);

    err = tibrvQueue_Create(&waitQueue);

    if (err != TIBRV_OK)
    {
        fprintf(stderr, "%s: Failed to create waiting queue: %s\n",
                progname, tibrvStatus_GetText(err));
        exit(1);
    }


    /*
     * Listen to each subject.
     */

    err = tibrvEvent_CreateVectorListener(&vectorListenerId_1, TIBRV_DEFAULT_QUEUE,
                                            vectorcallback_1, transport,
                                            "a.>", closure_1);

    if(err == TIBRV_OK)
    {
        printf("Created vector listener with subject a.> that gets handled by vectorcallback_1\n");
    }
    else
    {
        fprintf(stderr, "%s: Error %s listening to \"%s\"\n",
                progname, tibrvStatus_GetText(err), "a.>");
        exit(1);
    }


    err = tibrvEvent_CreateVectorListener(&vectorListenerId_2, TIBRV_DEFAULT_QUEUE,
                                            vectorcallback_1, transport,
                                            "b.>", closure_2);

    if(err == TIBRV_OK)
    {
        printf("Created vector listener with subject b.> that gets handled by vectorcallback_1\n");
    }
    else
    {
        fprintf(stderr, "%s: Error %s listening to \"%s\"\n",
                progname, tibrvStatus_GetText(err), "b.>");
        exit(1);
    }

    err = tibrvEvent_CreateVectorListener(&vectorListenerId_3, TIBRV_DEFAULT_QUEUE,
                                            vectorcallback_2, transport,
                                            "c.>", closure_3);

    if(err == TIBRV_OK)
    {
        printf("Created vector listener with subject c.> that gets handled by vectorcallback_2\n");
    }
    else
    {
        fprintf(stderr, "%s: Error %s listening to \"%s\"\n",
                progname, tibrvStatus_GetText(err), "c.>");
        exit(1);
    }


    err = tibrvEvent_CreateListener(&simpleListenerId, TIBRV_DEFAULT_QUEUE,
                                     simplecallback, transport,
                                     "a.1", closure_4);

    if(err == TIBRV_OK)
    {
        printf("Created listener with subject a.1 that gets handled by simplecallback\n");
    }
    else
    {
        fprintf(stderr, "%s: Error %s listening to \"%s\"\n",
                progname, tibrvStatus_GetText(err), "a.1");
        exit(1);
    }

    printf("Ready to receive message\n");
    /*
    * Dispatch loop - dispatches events which have been placed on the event queue
    */

    do{
        err  =  tibrvQueue_TimedDispatch(TIBRV_DEFAULT_QUEUE,TIBRV_WAIT_FOREVER);
        tibrvQueue_TimedDispatch(waitQueue,(tibrv_f64)1.0);
    }while(err == TIBRV_OK);

    tibrv_Close();

    free(closure_1);
    free(closure_2);
    free(closure_3);
    free(closure_4);
    return 0;
}
