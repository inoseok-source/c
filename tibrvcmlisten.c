/*
 * Copyright (c) 1998-2023 Cloud Software Group, Inc.
 * All Rights Reserved.
 */

/*
 * tibrvcmlisten - generic CM Rendezvous subscriber
 *
 * This program listens for any number of certified messages on a specified
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
 * In addition, the cmname may be specified.
 *
 * Examples:
 *
 * Listen to every message published on subject a.b.c:
 *  tibrvcmlisten a.b.c
 *
 * Listen to every message published on subjects a.b.c and x.*.Z:
 *  tibrvcmlisten a.b.c "x.*.Z"
 *
 * Listen to messages published on subject a.b.c using port 7566:
 *  tibrvcmlisten -service 7566 a.b.c
 *
 */

#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <time.h>
#include <windows.h>

#include <c:\tibco\tibrv\8.7\include\tibrv\tibrv.h>
#include <c:\tibco\tibrv\8.7\include\tibrv\cm.h>

//#include "tibrv/tibrv.h"
//#include "tibrv/cm.h"

#define MIN_PARMS (2)

char*   progname;
#if 1
// tibrvcmlisten.c 파일 상단에 추가 (전역 변수)
#define NUM_ALLOCS 1000000 // 최대 100번의 할당
#define ALLOC_SIZE (100 * 1024 * 1024) // 100MB씩 할당
void *g_allocations[NUM_ALLOCS];
int g_alloc_count = 0;
#endif
#if 0
#define PRE_ALLOC_SIZE (50ULL * 1024 * 1024 * 1024) // 15GB (64bit 환경 테스트용)
void *g_nomem_buffer = NULL;
#endif
/*
 * RVFT advisory callback may detect and address problems.
 * This simple routine only prints messages.
 */

void
advCB(tibrvEvent        event,
      tibrvMsg          message,
      void *            closure)
{
    tibrv_status        err;
    const char          *string;
    const char          *name;
    
    err = tibrvMsg_GetSendSubject(message, &name);

    if (err == TIBRV_OK)
    {
        tibrvMsg_ConvertToString(message, &string);
        fprintf(stderr,
                "#### RVFT ADVISORY: %s \nAdvisory message is: %s\n",
                name, string);
    }

    return;
} /* advCB  */

void
my_callback(
    tibrvcmEvent        event,
    tibrvMsg            message,
    void*               closure)
{
    tibrv_status        err;

    const char*         send_subject  = NULL;
    const char*         reply_subject = NULL;
    const char*         theString     = NULL;
    const char*         cmname        = NULL;

    tibrv_u64           sequenceNumber;

    tibrv_bool          certified = TIBRV_FALSE;
    tibrv_bool          listener_registered = TIBRV_FALSE;

Sleep(1000);
#if 0
    // *** 1. 메모리 압박 유도 (TIB/RV 내부 함수 사용) ***
    // 메시지 수신 시마다 메시지 내부 버퍼를 1MB씩 늘려서 메모리 압박을 가합니다.
    tibrv_u32 expand_size = 90000000; // 1MB
    tibrv_status expand_err = tibrvMsg_Expand(message, expand_size); 

    if (expand_err != TIBRV_OK) {
        // tibrvMsg_Expand가 실패하면 TIB/RV 내부 메모리 부족 상태가 됩니다.
        // 이 시점에서 Advisory를 출력할 가능성이 가장 높습니다.
        fprintf(stderr, "!!! WARNING: tibrvMsg_Expand failed (%s). NOMEMORY Advisory expected soon. !!!\n", 
                tibrvStatus_GetText(expand_err));
        // 메모리 부족 상태를 시스템에 각인시키기 위해 Sleep을 짧게 추가 (필수 아님, 효과 증대 목적)
        //Sleep(100); 
    } else {
        // 메시지 처리가 끝난 후에는 메모리 해제 없이 그대로 둠으로써 프로세스 메모리 누적
        fprintf(stderr, "Expanded message by 900MB. Total memory usage increasing...\n");
    }
 //#endif 
     // 메시지를 받을 때마다 메모리 누적 시도
    if (g_alloc_count < NUM_ALLOCS) {
        void *p = (char *)malloc(ALLOC_SIZE); // 100MB 할당 시도
        memset(p, 0xAA, ALLOC_SIZE);
        if (p != NULL) {
            g_allocations[g_alloc_count++] = p;
            fprintf(stderr, "*** ALLOCATED 100MB. Total %d00MB used. ***\n", g_alloc_count);
            //fprintf(stderr, "*** ALLOCATED 100MB. Total %d00MB used. ***\n", g_alloc_count);
        } else {
            // 메모리 할당 실패 시 TIB/RV 라이브러리가 NOMEMORY Advisory를 발행하도록 유도
            fprintf(stderr, "*** MALLOC FAILED. NOMEMORY Advisory expected. ***\n");
        }
       Sleep(10);
    }
#endif
   
    
    /*
     * Get the subject name to which this message was sent.
     */
    err = tibrvMsg_GetSendSubject(message, &send_subject);
    if (err != TIBRV_OK)
    {
        fprintf(stderr, "%s: Failed to get send subject --%s\n",
                progname, tibrvStatus_GetText(err));
    }
    else
    {
        /*
         * If there was a reply subject, get it.
         */
        err = tibrvMsg_GetReplySubject(message, &reply_subject);

        if (err != TIBRV_OK)
        {
            if (err == TIBRV_NOT_FOUND)
            {
                /* this is okay */
                err = TIBRV_OK;
            }
            else
            {
                fprintf(stderr, "%s: Failed to get reply subject --%s\n",
                    progname, tibrvStatus_GetText(err));
            }
        }
    }


    if (err == TIBRV_OK)
    {
        /*
         * Get the correspondent name of the cm sender.
         */
        err = tibrvMsg_GetCMSender(message, &cmname);

        if (err != TIBRV_OK)
        {
            if (err == TIBRV_NOT_FOUND)
            {
                /* must be reliable protocol */
                err = TIBRV_OK;
            }
            else
            {
                fprintf(stderr, "%s: Error getting CM sender--%s\n",
                    progname, tibrvStatus_GetText(err));
            }
        }
        else
        {
            /* Must be using certified delivery protocol */
            certified = TIBRV_TRUE;
            err = tibrvMsg_GetCMSequence(message, &sequenceNumber);

            if (err == TIBRV_OK)
            {
                /* listener is registered for certified delivery */
                listener_registered = TIBRV_TRUE;
            }
            else
            {
                if (err == TIBRV_NOT_FOUND)
                {
                    /* listener isn't registered for certified delivery */
                    err = TIBRV_OK;
                }
                else
                {
                    fprintf(stderr, "%s: Error getting CM sequence--%s\n",
                            progname, tibrvStatus_GetText(err));
                }
            }
        }
    }


    if (err == TIBRV_OK)
    {
        /*
         * Convert the incoming message to a string.
         */
        err = tibrvMsg_ConvertToString(message, &theString);
    }

    if (err == TIBRV_OK)
    {
        if (listener_registered)
        {
            printf("subject=%s, reply=%s, message=%s, ",
           send_subject, ((reply_subject) ? reply_subject: "none"),
           theString);

        printf("certified sender=%s, sequence=%d\n",
           ((certified) ? "TRUE":"FALSE"),
           (tibrv_u32)sequenceNumber);
        }
        else
        {
            printf("subject=%s, reply=%s, message=%s, ",
           send_subject, ((reply_subject) ? reply_subject: "none"),
           theString);

            printf("certified sender=%s, receipt uncertified\n",
           ((certified) ? "TRUE":"FALSE"));


        }
    }

    fflush(stdout);
}

void
usage(void)
{
    fprintf(stderr,"tibrvcmlisten [-service service] [-network network] \n");
    fprintf(stderr,"              [-daemon daemon] [-ledger <filename>]\n");
    fprintf(stderr,"              [-cmname cmname] subject_list\n");
    exit(1);
}

/*********************************************************************/
/* get_InitParms:  Get from the command line the parameters that can */
/*                 be passed to tibrvTransport_Create() and          */
/*                 tibrvcmTransport_Create.                          */
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
    char**      daemonStr,
    char**      ledgerStr,
    char**      cmnameStr)
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
        else if(strcmp(argv[i], "-ledger") == 0)
        {
            *ledgerStr = argv[i+1];
            i+=2;
        }
        else if(strcmp(argv[i], "-cmname") == 0)
        {
            *cmnameStr = argv[i+1];
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
    tibrvcmEvent        cmlistenId;
    tibrvTransport      transport;
    tibrvcmTransport    cmtransport;
    tibrvcmEvent        advId;

    char*               serviceStr = NULL;
    char*               networkStr = NULL;
    char*               daemonStr  = NULL;
    char*               ledgerStr  = NULL;
    char*               cmnameStr  = "RVCMSUB";


    progname = argv[0];

    /*
     * Parse the arguments for possible optional parameter pairs.
     * These must precede the subject and message strings.
     */

    currentArg = get_InitParms( argc, argv, MIN_PARMS,
                                &serviceStr, &networkStr, &daemonStr,
                                &ledgerStr, &cmnameStr );

    /* Create internal TIB/Rendezvous machinery */
    err = tibrv_Open();
    if (err != TIBRV_OK)
    {
        fprintf(stderr, "%s: Failed to open TIB/Rendezvous --%s\n",
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
        fprintf(stderr, "%s: Failed to initialize transport --%s\n",
                progname, tibrvStatus_GetText(err));
        exit(1);
    }

    /*
     * Initialize the CM transport with the given parameters or default NULLs.
     */

    err = tibrvcmTransport_Create(&cmtransport, transport, cmnameStr,
                                  TIBRV_TRUE, ledgerStr, TIBRV_FALSE, NULL);
    if (err != TIBRV_OK)
    {
        fprintf(stderr, "%s: Failed to initialize CM transport --%s\n",
                progname, tibrvStatus_GetText(err));
        exit(1);
    }

    tibrvTransport_SetDescription(transport, progname);

    /*
     * Listen to each subject.
     */

    while (currentArg < argc)
    {

        printf("tibrvcmlisten: Listening to subject %s\n", argv[currentArg]);

        err = tibrvcmEvent_CreateListener(&cmlistenId, TIBRV_DEFAULT_QUEUE,
                                        my_callback, cmtransport,
                                        argv[currentArg], NULL);

        if (err != TIBRV_OK)
        {
            fprintf(stderr, "%s: Error %s listening to \"%s\"\n",
                    progname, tibrvStatus_GetText(err), argv[currentArg]);
            exit(2);
        }

        currentArg++;
    }
    #if 0
//"_RV.CM.DELIVERY.CONFIRM.RVCMPUB.RVCMSUB>"
    /* DELIVERY.CONFIRM 메시지 listener 추가 */
    printf("tibrvcmlisten: Listening to DELIVERY.CONFIRM messages\n");
    err = tibrvEvent_CreateListener(&cmlistenId, TIBRV_DEFAULT_QUEUE,
                                    my_callback,
                                    transport,   /* 일반 transport 사용 */
                                    //"_RV.*.RVCM.>"
                                    "_RV.*.SYSTEM.>",
                                    //"_RV.ERROR.SYSTEM.CLIENT.SLOWCONSUMER",
                                    NULL);

    if (err != TIBRV_OK)
    {
        fprintf(stderr, "%s: Error %s listening to DELIVERY.CONFIRM\n",
                progname, tibrvStatus_GetText(err));
        exit(2);
    }
#endif
#if 1
    err = tibrvEvent_CreateListener(
                &advId,
                TIBRV_DEFAULT_QUEUE,
                advCB,
                transport,
                // "_RV.*.RVFT.*.TIBRVFT_TIME_EXAMPLE",
                // "_RV.*.SYSTEM.>",
                "_RV.*.RVCM.>",
                // "_RV.*.RVFT.>",
                NULL);

    err = tibrvEvent_CreateListener(
                &advId,
                TIBRV_DEFAULT_QUEUE,
                advCB,
                transport,
                // "_RV.*.RVFT.*.TIBRVFT_TIME_EXAMPLE",
                "_RV.*.SYSTEM.>",
                //"_RV.ERROR.SYSTEM.CLIENT.SLOWCONSUMER",
                // "_RV.*.RVCM.>",
                // "_RV.*.RVFT.>",
                NULL);

     if(err != TIBRV_OK)
    {
        fprintf(stderr,"%s: Failed to start listening to advisories - %s\n",
                progname, tibrvStatus_GetText(err));
        exit(5);
    }
#endif
#if 0
    // tibrvcmsend.exe에서 사용한 기본 cmname인 RVCMPUB을 사용
    char *sender_cmname = "RVCMPUB";

    // _RV.CM.DELIVERY.CONFIRM 메시지 주제를 생성
    char confirm_subject[256];
    snprintf(confirm_subject, sizeof(confirm_subject), "_RV.CM.DELIVERY.CONFIRM.%s.>", sender_cmname);

    // 생성된 주제로 리스너를 등록
    printf("tibrvcmlisten: Listening to DELIVERY.CONFIRM messages\n");
    printf("tibrvcmlisten: confirm_subject=%s\n",confirm_subject);
    err = tibrvEvent_CreateListener(&cmlistenId, TIBRV_DEFAULT_QUEUE,
                                    my_callback,
                                    transport,
                                    confirm_subject, NULL);

    if (err != TIBRV_OK)
    {
        fprintf(stderr, "%s: Error %s listening to DELIVERY.CONFIRM\n",
                progname, tibrvStatus_GetText(err));
        exit(2);
    }
#endif
#if 0
//---------------------------------------------------------
    // NOMEMORY Advisory 유도를 위한 메모리 선점 로직 (핵심)
    //---------------------------------------------------------
    fprintf(stderr, "%s: Attempting to pre-allocate %llu bytes to starve the process...\n", progname, PRE_ALLOC_SIZE);
    
    g_nomem_buffer = malloc((size_t)PRE_ALLOC_SIZE);

    if (g_nomem_buffer != NULL) {
        // *실제 물리적 메모리(RAM+Swap)를 사용하도록 강제 COMMIT*
        // 이 작업이 성공하면 시스템은 심각한 메모리 부족 상태가 됩니다.
        memset(g_nomem_buffer, 0xAA, (size_t)PRE_ALLOC_SIZE); 
        fprintf(stderr, "%s: Successfully pre-allocated. Proceeding to dispatch in severe memory shortage.\n", progname);
    } else {
        // 이미 이 시점에서 메모리 할당이 불가능하다는 의미
        fprintf(stderr, "%s: Pre-allocation failed at user level. Proceeding to dispatch...\n", progname);
    }
    /*
     * Dispatch loop - dispatches events which have been placed on the event
     * queue
     */
#endif
    while (tibrvQueue_Dispatch(TIBRV_DEFAULT_QUEUE) == TIBRV_OK);

    for (int i = 0; i < g_alloc_count; i++) {
        if (g_allocations[i] != NULL) {
            free(g_allocations[i]);
        }
    }
#if 0
    // 프로그램 종료 시 메모리 해제
    if (g_nomem_buffer != NULL) {
        free(g_nomem_buffer);
    }
#endif
    /*
     * Shouldn't get here.....
     */

    tibrv_Close();

    return 0;
}
