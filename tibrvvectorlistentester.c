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
#include <string.h>

#include "tibrv/tibrv.h"

#define MIN_PARMS       (3)
static tibrv_u32                sendCount = 1;
void
usage(void)
{
    fprintf(stderr,"tibrvvectorlistenertester   [-service service] [-network network] \n");
    fprintf(stderr,"                            [-daemon daemon] [-messages <messages>] \n");
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
    int         min_parms,
    char**      serviceStr,
    char**      networkStr,
    char**      daemonStr)
{
    int i=1;

    char * end;
    while ( i+2 <= argc && *argv[i] == '-' )
    {
        if (strcmp(argv[i], "-service") == 0)
        {
            *serviceStr = argv[i+1];
            i+=2;
        }
        else if (strcmp(argv[i], "-network") == 0)
        {
            *networkStr = argv[i+1];
            i+=2;
        }
        else if (strcmp(argv[i], "-daemon") == 0)
        {
            *daemonStr = argv[i+1];
            i+=2;
        }
        else if (strcmp(argv[i], "-messages") == 0)
        {
            if (++i >= argc)
            usage();
            sendCount = strtoul(argv[i], &end, 10);
        }
        else
        {
            usage();
        }
    }

    return( i );
}

int
main(int argc, char **argv)
{
    tibrv_status        err;
    tibrvTransport      transport;
    tibrvMsg            message[1000];

    char*               serviceStr = NULL;
    char*               networkStr = NULL;
    char*               daemonStr  = NULL;

    char*               progname = argv[0];
    tibrv_u32           sent = 0;
    int                 i;
    int                 index = 0;
    /*
     * Parse arguments for possible optional parameter pairs.
     * These must precede the subject and message strings.
     */
    get_InitParms(argc, argv, MIN_PARMS, &serviceStr, &networkStr, &daemonStr);

    /*
     * Create internal TIB/Rendezvous machinery
     */
    err = tibrv_Open();
    if (err != TIBRV_OK)
    {
        fprintf(stderr, "%s: Failed to open TIB/RV: %s\n",
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

    /*
     * Create messages
     */
    for(i=0;i < 90;i++)
    {
        err = tibrvMsg_Create(&message[i]);
        if (err != TIBRV_OK)
        {
            fprintf(stderr, "%s: Failed to create message: %s\n",
                    progname, tibrvStatus_GetText(err));
            exit(1);
        }
    }


    for(i=0;i < 90;i++)
    {
        /* set the following subjects
            * a.2, a.3, b.1, b.2, b.3, a.1, a.4, b.4, c.1
            */
        index = i%9;
        switch(index)
        {
        case 0:
            err = tibrvMsg_SetSendSubject(message[i], "a.2");
            break;
        case 1:
            err = tibrvMsg_SetSendSubject(message[i], "a.3");
            break;
        case 2:
            err = tibrvMsg_SetSendSubject(message[i], "b.1");
            break;
        case 3:
            err = tibrvMsg_SetSendSubject(message[i], "b.2");
            break;
        case 4:
            err = tibrvMsg_SetSendSubject(message[i], "b.3");
            break;
        case 5:
            err = tibrvMsg_SetSendSubject(message[i], "a.1");
            break;
        case 6:
            err = tibrvMsg_SetSendSubject(message[i], "a.4");
            break;
        case 7:
            err = tibrvMsg_SetSendSubject(message[i], "b.4");
            break;
        case 8:
            err = tibrvMsg_SetSendSubject(message[i], "c.1");
            break;
        default:
            err = tibrvMsg_SetSendSubject(message[i], "hello");
            break;

        }

        if (err != TIBRV_OK)
        {
            fprintf(stderr, "%s: Failed to set send subject in program : %s\n",
                    progname, tibrvStatus_GetText(err));
            exit(1);
        }
    }



    while(sent < sendCount)
    {

        err = tibrvTransport_Sendv(transport, message, 90);

        if ( err != TIBRV_OK)
        {
            fprintf(stderr, "%s: %s \n",
                    progname, tibrvStatus_GetText(err));
            break;
        }
        printf("Published: all messages \n");
        sent++;
    }

    /*
     * Destroy message
     */
    for(i=0;i<90;i++)
    {
        tibrvMsg_Destroy(message[i]);
    }
    /*
     * tibrv_Close() will destroy the transport and guarantee delivery.
     */

    tibrv_Close();

    exit(0);
}
