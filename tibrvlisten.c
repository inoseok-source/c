/*
 * Copyright (c) 1998-2023 Cloud Software Group, Inc.
 * All Rights Reserved.
 */

/*
 * tibrvlisten - generic Rendezvous subscriber
 *
 * This program listens for any number of messages on a specified
 * set of subject(s).  Message(s) received are printed.
 *
 * Some platforms require proper quoting of the arguments to prevent
 * the command line processor from modifying the command arguments.
 *
 * The user may terminate the program by typing Control-C.
 *
 * Optionally the user may specify communication parameters for
 * tibrvTransport_Create.  If none are specified, default values
 * are used.  For information on default values for these parameters,
 * please see the TIBCO/Rendezvous Concepts manual.
 *
 * Examples:
 *
 * Listen to every message published on subject a.b.c:
 *  tibrvlisten a.b.c
 *
 * Listen to every message published on subjects a.b.c and x.*.Z:
 *  tibrvlisten a.b.c "x.*.Z"
 *
 * Listen to every system advisory message:
 *  tibrvlisten "_RV.*.SYSTEM.>"
 *
 * Listen to messages published on subject a.b.c using port 7566:
 *  tibrvlisten -service 7566 a.b.c
 *
 */

#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <time.h>

#include "tibrv/tibrv.h"

#define MIN_PARMS (2)

void
my_callback(
    tibrvEvent          event,
    tibrvMsg            message,
    void*               closure)
{
    const char*         send_subject  = NULL;
    const char*         reply_subject = NULL;
    const char*         theString     = NULL;
    char		localTime[TIBRVMSG_DATETIME_STRING_SIZE];
    char		gmtTime[TIBRVMSG_DATETIME_STRING_SIZE];

    /*
     * Get the subject name to which this message was sent.
     */
    tibrvMsg_GetSendSubject(message, &send_subject);

    /*
     * If there was a reply subject, get it.
     */
    tibrvMsg_GetReplySubject(message, &reply_subject);

    /*
     * Convert the incoming message to a string.
     */
    tibrvMsg_ConvertToString(message, &theString);

    tibrvMsg_GetCurrentTimeString(localTime, gmtTime);

    if (reply_subject)
        printf("%s (%s): subject=%s, reply=%s, message=%s\n",
               localTime, gmtTime, send_subject, reply_subject, theString);
    else
        printf("%s (%s): subject=%s, message=%s\n",
               localTime, gmtTime, send_subject, theString);

    fflush(stdout);
}

void
usage(void)
{
    fprintf(stderr,"tibrvlisten [-service service] [-network network] \n");
    fprintf(stderr,"            [-daemon daemon] subject_list\n");
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
    int i = 1;

    if ( argc < min_parms )
        usage();

    while ( i+2 <= argc && *argv[i] == '-' )
    {
        if(strcmp(argv[i], "-service") == 0)
        {
            *serviceStr = argv[i+1];
            i+=2;
        }
        else if(strcmp(argv[i], "-network") == 0)
        {
            *networkStr = argv[i+1];
            i+=2;
        }
        else if(strcmp(argv[i], "-daemon") == 0)
        {
            *daemonStr = argv[i+1];
            i+=2;
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
    int                 currentArg;
    tibrvEvent          listenId;
    tibrvTransport      transport;

    char*               serviceStr = NULL;
    char*               networkStr = NULL;
    char*               daemonStr  = NULL;

    char*               progname = argv[0];

    /*
     * Parse the arguments for possible optional parameter pairs.
     * These must precede the subject and message strings.
     */

    currentArg = get_InitParms( argc, argv, MIN_PARMS,
                                &serviceStr, &networkStr, &daemonStr );

    /* Create internal TIB/Rendezvous machinery */
    if (tibrv_IsIPM())
    {
        /*
         * Prior to using the Rendezvous IPM library please read the appropriate
         * sections of user guide to determine if IPM is the correct choice for your
         * application; it is likely not.
         *
         * To use the shared Rendezvous IPM library in C on supported platforms,
         * first make sure it is located in your library path before the standard
         * Rendezvous library.
         *
         * The IPM shared library can be found in $TIBRV_HOME/lib/ipm (or
         * $TIBRV_HOME/bin/ipm for the Windows DLL)
         *
         * The IPM static library can be found in $TIBRV_HOME/lib/ipm.
         *
         * To configure IPM you can do one of the following:
         *
         * 1) Nothing, and accept the default IPM RV parameter values.
         *
         * 2) Place a file named "tibrvipm.cfg" in your PATH, and have
         * IPM automatically read in configuration values.
         *
         * 3) Call tibrv_SetRVParameters, prior to tibrv_Open:
         *
         *   const char* rvParams[] = {"-reliability", "3", "-reuse-port", "30000"};
         *   tibrv_SetRVParameters(sizeof(rvParams)/sizeof(char*), rvParams);
         *   tibrv_Open();
         *
         * 4) Call tibrv_OpenEx, and have IPM read in the configuration values:
         *
         *   char* cfgfile = "/var/tmp/mycfgfile"
         *   tibrv_OpenEx(cfgfile);
         *
         * An example configuration file, "tibrvipm.cfg", can be found in the
         * "$TIBRV_HOME/examples/IPM directory" of the Rendezvous installation.
         *
         */

        const char* rvParams[] = {"-reliability", "3"};
        
        err = tibrv_SetRVParameters(sizeof(rvParams)/sizeof(char*), rvParams);
        
        if (err != TIBRV_OK)
        {
            fprintf(stderr, "%s: Failed to set TIB/Rendezvous parameters for "
                    "IPM: %s\n", progname, tibrvStatus_GetText(err));
            exit(1);
        }
    }
    
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

    /*
     * Listen to each subject.
     */

    while (currentArg < argc)
    {

        printf("tibrvlisten: Listening to subject %s\n", argv[currentArg]);

        err = tibrvEvent_CreateListener(&listenId, TIBRV_DEFAULT_QUEUE,
                                        my_callback, transport,
                                        argv[currentArg], NULL);

        if (err != TIBRV_OK)
        {
            fprintf(stderr, "%s: Error %s listening to \"%s\"\n",
                    progname, tibrvStatus_GetText(err), argv[currentArg]);
            exit(2);
        }

        currentArg++;
    }

    /*
     * Dispatch loop - dispatches events which have been placed on the event queue
     */


    while (tibrvQueue_Dispatch(TIBRV_DEFAULT_QUEUE) == TIBRV_OK);

    /*
     * Shouldn't get here.....
     */

    tibrv_Close();

    return 0;
}
