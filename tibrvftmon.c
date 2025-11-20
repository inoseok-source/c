/*
 * Copyright (c) 1998-2023 Cloud Software Group, Inc.
 * All Rights Reserved.
 */

/*
 * tibrvftmon.c - example TIB/Rendezvous fault tolerant group
 *                monitor program
 *
 * This program monitors the fault tolerant group TIBRVFT_TIME_EXAMPLE,
 * the group established by the tibrvfttime timestamp message sending
 * program.   It will report a change in the number of active members
 * of that group.
 *
 * The tibrvfttime program must use the default communication
 * parameters.
 */


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>

#include "tibrv/tibrv.h"
#include "tibrv/ft.h"

/*
 * Fault tolerance monitor callback called when TIBRVFT detects a
 * change in the number of active members in group TIBRVFT_TIME_EXAMPLE.
 */

static void
monCB(
    tibrvftMonitor      monitor,
    const char*         groupName,
    tibrv_u32           numActiveMembers,
    void*               closure)
{
    static unsigned long        oldNumActives = 0;

    printf("Group [%s]: has %d active members (after %s).\n",
           groupName,
           numActiveMembers,
           (oldNumActives > numActiveMembers) ?
           "one deactivated" : "one activated");

    oldNumActives = numActiveMembers;

    return;
}

void
usage(void)
{
    fprintf(stderr,"tibrvftmon [-service service] [-network network] \n");
    fprintf(stderr,"           [-daemon daemon] \n");
    exit(1);
}

/*********************************************************************/
/* get_InitParms:  Get from the command line the parameters that can */
/*                 be passed to tibrvTransport_Create().             */
/*                                                                   */
/*********************************************************************/
void
get_InitParms(
    int         argc,
    char*       argv[],
    char**      serviceStr,
    char**      networkStr,
    char**      daemonStr)
{
    int i=1;

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
        else
        {
            usage();
        }
    }

}

int
main( int argc, char **argv)
{
    tibrv_status        err;
    tibrvTransport      transport;
    tibrvftMonitor      monitor;
    tibrv_f64           lostInt = 4.8;  /* matches tibrvfttime */

    char*               serviceStr = NULL;
    char*               networkStr = NULL;
    char*               daemonStr  = NULL;

    /*
     * Parse the arguments for possible optional parameter pairs.
     */

    get_InitParms( argc, argv, &serviceStr, 
                               &networkStr, &daemonStr );

    /*
     * Create internal TIB/Rendezvous machinery
     */
    err = tibrv_Open();
    if (err != TIBRV_OK)
    {
        fprintf(stderr, "%s: Failed to open TIB/RV --%s\n",
                argv[0], tibrvStatus_GetText(err));
        exit(1);
    }

    /*
     * Initialize the transport with the given parameters or default NULLs.
     */

    err = tibrvTransport_Create(&transport, serviceStr,
                                networkStr, daemonStr);
    if (err != TIBRV_OK)
    {
        fprintf(stderr, "%s: Failed to initialize transport --%s\n",
                argv[0], tibrvStatus_GetText(err));
        exit(1);
    }


    /* Set up the monitoring of the RVFT_TIME_EXAMPLE group */

    err = tibrvftMonitor_Create(
                &monitor,
                TIBRV_DEFAULT_QUEUE,
                monCB,
                transport,
                "TIBRVFT_TIME_EXAMPLE",
                lostInt,
                NULL);


    if(err != TIBRV_OK)
    {
        fprintf(stderr,
                "%s: Failed to start group monitor - %s\n", argv[0],
                tibrvStatus_GetText(err));
        exit(1);
    }

    fprintf(stderr,"%s: Waiting for group information...\n", argv[0]);

    /* Dispatch loop - dispatches events which have been placed on the event queue */

    while (tibrvQueue_Dispatch(TIBRV_DEFAULT_QUEUE) == TIBRV_OK)
    {
        /* over and over again */
    }

    /*
     * Shouldn't get here.....
     */
    tibrvftMonitor_Destroy(monitor);
    tibrv_Close();

    exit(0);
}
