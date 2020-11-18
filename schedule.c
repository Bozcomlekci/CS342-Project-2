#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <time.h>
#include <mqueue.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h> 
#include <errno.h> 
#include <sys/stat.h>
#include <sys/wait.h>
#include <stdlib.h>
#include <pthread.h>
#include <limits.h>

#define TRACE 1


#define BUFSIZE  INT_MAX
#define MAXFILENAME 128
#define BURSTMAX INT_MAX

void *producer (void *param);
struct timeval timer;

struct Node{
	long long burstTime;
	struct Node* next;
	int pid;
	long long timeQueued;
	int responded;
};

struct Queue { 
	struct Node *head, *tail; 
	int count;
}; 

struct tdata {
   	int pid; 
	char inputname[MAXFILENAME];
};

char* itoa(int val, int base){
	
	static char buf[32] = {0};
	
	int i = 30;
	
	for(; val && i ; --i, val /= base)
		buf[i] = "0123456789abcdef"[val % base];
	
	return &buf[i+1];
	
}


void queue_init(struct Queue *q)
{
        q->count = 0;
        q->head = NULL;
        q->tail = NULL;
}


void queue_insert(struct Queue *q, struct Node *qe)
{

        if (q->count == 0) {
                q->head = qe;
                q->tail = qe;
        } else {
                q->tail->next = qe;
                q->tail = qe;
        }

        q->count++;
}


struct Node* queue_retrieve(struct Queue *q)
{
        struct Node *qe;

        if (q->count == 0)
                return NULL;

        qe = q->head;
        q->head = q->head->next;
        q->count--;

        return (qe);
}


struct Node* queue_retrieve_withSJF(struct Queue *q)
{
        struct Node *qe;

        if (q->count == 0)
                return NULL;
	
	if (q->count == 1)  {
		qe = q->head;
		q->head = NULL;
		q->tail = NULL;
        }
        else {
		struct Node* curr = q->head;
		int indexOfSmallest = 1;
		int i = 1;
		int tempBurstTime = q->head->burstTime;
		while (curr != NULL) {
			if( curr->burstTime < tempBurstTime){
				indexOfSmallest = i;
				tempBurstTime = curr->burstTime;
			}
			i++;
			curr = curr->next;			
		}
		if(indexOfSmallest == 1){
			qe = q->head;
			q->head = q->head->next;
		}
		else {
			struct Node* prev;
			prev = q->head;
			curr = prev->next;			
			for(i=2; i<indexOfSmallest; i++)
			{
				prev = prev->next;
				curr = curr->next;
			}
			qe = curr;
			prev->next = curr->next;
			if(indexOfSmallest == q->count){
				q->tail = prev;
			}
		}		
		qe->next = NULL;
        }
        q->count--;
        return (qe);
}

long long randomBurst(long long min, long long max) {
	return rand()%((max+1)-min) + min;
}




// This is the shared object
struct buffer {
        struct Queue *q;               /*queue*/
        pthread_mutex_t th_mutex_process; /*mutex to prevent process to move on with IO burst before execution*/
        pthread_mutex_t th_mutex_queue;   /* mutex  to protect queue */
        pthread_cond_t  th_cond_hasspace;  /* will cause insert to wait */
        pthread_cond_t  th_cond_hasitem;   /* will cause remove to wait */
        pthread_cond_t  th_cond_burstCompleted[5]; /* will cause PSs to wait*/
};

void queue_add(struct buffer* qbuffer, struct Node *qep)
{
	pthread_mutex_lock(&qbuffer->th_mutex_queue);
		
	/* critical section begin */
	gettimeofday(&timer, NULL);
	qep->timeQueued = timer.tv_usec + 1000000*timer.tv_sec;
	
	while (qbuffer->q->count == BUFSIZE)
		pthread_cond_wait(&qbuffer->th_cond_hasspace, &qbuffer->th_mutex_queue);

	queue_insert(qbuffer->q, qep); 

	if (TRACE) {
		printf ("Insert process %d with  CPU burst = %lld to the queue\n", qep->pid, qep->burstTime);
		fflush (stdout);
	}
		
	if (qbuffer->q->count == 1)
		pthread_cond_signal(&qbuffer->th_cond_hasitem);

	/* critical section end */
	pthread_mutex_unlock(&qbuffer->th_mutex_queue);
}


struct Node* queue_remove(struct buffer *qbuffer)
{
	struct Node *qe;
	
	pthread_mutex_lock(&qbuffer->th_mutex_queue);

	/* critical section begin */
	
	while (qbuffer->q->count == 0) {
		pthread_cond_wait(&qbuffer->th_cond_hasitem,
				  &qbuffer->th_mutex_queue);
	}

	qe = queue_retrieve(qbuffer->q);
	
	if (qe == NULL) {
		printf("can not retrieve; should not happen\n");
		exit(1);
	}
	
	if (TRACE) {
		printf ("SE retrieved the process %d with CPU burst = %lld\n", qe->pid,  qe->burstTime);
		fflush (stdout);
	}
	
	if (qbuffer->q->count == (BUFSIZE - 1))
		pthread_cond_signal(&qbuffer->th_cond_hasspace);
	
	/* critical section end */
	
	pthread_mutex_unlock(&qbuffer->th_mutex_queue);
	return (qe); 
}

struct Node* queue_remove_withSJF(struct buffer *qbuffer)
{
	struct Node *qe;
	
	pthread_mutex_lock(&qbuffer->th_mutex_queue);

	/* critical section begin */
	
	while (qbuffer->q->count == 0) {
		pthread_cond_wait(&qbuffer->th_cond_hasitem,
				  &qbuffer->th_mutex_queue);
	}

	qe = queue_retrieve_withSJF(qbuffer->q);
	
	if (qe == NULL) {
		printf("can not retrieve; should not happen\n");
		exit(1);
	}
	
	if (TRACE) {
		printf ("SE retrieved the process %d with CPU burst = %lld\n", qe->pid,  qe->burstTime);;
		fflush (stdout);
	}
	
	if (qbuffer->q->count == (BUFSIZE - 1))
		pthread_cond_signal(&qbuffer->th_cond_hasspace);
	
	/* critical section end */
	
	pthread_mutex_unlock(&qbuffer->th_mutex_queue);
	return (qe); 
}

 
/*********** GLOBAL VARIABLES ******************************************/
char outfilename[MAXFILENAME];
int N;
long long minCPU; 
long long maxCPU;
long long minIO;
long long maxIO;
long long duration;
char* algorithm;
char infilePrefix[MAXFILENAME];
long long quantum;

int finishedInfiles;

struct buffer *qbuffer;
/**********************************************************************/

int main(int argc, char **argv){
	N = atoi(argv[1]);
	minCPU = atoi(argv[2]); 
	maxCPU = atoi(argv[3]);
	minIO = atoi(argv[4]);
	maxIO = atoi(argv[5]);
	strcpy(outfilename, argv[6]); 
	duration = atoi(argv[7]);
	algorithm = argv[8];
	if ( strcmp("RR", algorithm ) != 0 ){
		quantum = 0;
	}
	else {
		quantum = atoi(argv[9]);
		if( quantum % 100 != 0 || quantum > 300 || quantum == 0 ){
			perror("Invalid Time Quantum");
			exit(1);
		}
	}
	strcpy(infilePrefix, argv[10]); 
		
	if ( argc < 10 ) {
		printf ("usage: schedule N minCPU maxCPU minIO maxIO outfile duration algorithm quantum infileprefix\n"); 
		exit (1); 
	}
	
	printf("N: %d\n", N);

	//Initialize buffer and mutex/condition variables
	qbuffer = (struct buffer *) malloc(sizeof (struct buffer));
	qbuffer->q = (struct Queue *) malloc(sizeof (struct Queue));
	queue_init(qbuffer->q);
	pthread_mutex_init(&qbuffer->th_mutex_process, NULL);
	pthread_mutex_init(&qbuffer->th_mutex_queue, NULL);
	pthread_cond_init(&qbuffer->th_cond_hasspace, NULL);
	pthread_cond_init(&qbuffer->th_cond_hasitem, NULL);
	
	//Initialize time measurements	
	long long totalWaitingTimes[N];
	long long averageResponseTimes[N];
	long long burstCounts[N];
	
	int j;
	for( j = 0; j < N; j++){
		//Initialize buffer and mutex/condition variables 
		pthread_cond_init(&qbuffer->th_cond_burstCompleted[j], NULL);
		
		//Initialize time measurements
		totalWaitingTimes[j] = 0;
		averageResponseTimes[j] = 0;
		burstCounts[j] = 0;
	}
	
	finishedInfiles = 0;
	srand(time(NULL));
	

		
	pthread_t thread[N];
	struct tdata thread_data[N];

	int i = 0;
	int success;
	
	for( i = 0; i < N; i++ ){
		thread_data[i].pid = i+1; 
		success = pthread_create( &thread[i], NULL, producer, (void*) &(thread_data[i]));
		if (success != 0) {
			perror ("thread create failed\n"); 
			exit (1);
		}
       }
       
        //Simulate scheduler and executer
        FILE *fp; 
	struct Node *qe;
	long long burstTotal = N*duration;
	fp = fopen( outfilename, "w+" );
	int isFirst = 1;
	struct timeval start;
	struct timeval beginTime;
	if( strcmp("SJF", algorithm ) == 0 ) {
		while (burstTotal != 0) {
			qe = queue_remove_withSJF(qbuffer);
			if( qe->pid == -1 ){
				finishedInfiles++;
			}
			else{
				long long startTime;
				if( isFirst == 1 )
				{
					gettimeofday( &start, NULL );
					gettimeofday( &beginTime, NULL );
					isFirst = 0; 
					startTime = 0;
				} 
				else {
					gettimeofday( &start, NULL );
					startTime = start.tv_usec + start.tv_sec*1000000 - beginTime.tv_usec - beginTime.tv_sec*1000000;
				}	
				fprintf (fp, "%010lld ", startTime); 
				
				gettimeofday( &timer, NULL );
				if( qe->responded == 0 )
				{
					qe->responded = 1;
					averageResponseTimes[qe->pid-1] += timer.tv_usec + timer.tv_sec*1000000 - qe->timeQueued;
					burstCounts[qe->pid-1] += 1;
				}	
				totalWaitingTimes[qe->pid-1] += timer.tv_sec*1000000+timer.tv_usec - qe->timeQueued;
				
				fprintf (fp, "%lld ", qe->burstTime); 
				fprintf (fp, "%d\n", qe->pid);
				fflush (fp); 
				usleep( (qe->burstTime)*1000 );
				pthread_cond_signal(&qbuffer->th_cond_burstCompleted[qe->pid-1]);
				free (qe); 
				burstTotal--;
			}
			if( finishedInfiles == N ){
				break;
			}
			
		}
	}
	else if( strcmp("RR", algorithm ) == 0)
	{
		
		while (burstTotal != 0) {
			qe = queue_remove(qbuffer); // one thread at a time
			if( qe->pid == -1 ){
				finishedInfiles++;
			}
			else {
				long long startTime;
				if( isFirst == 1 )
				{
					gettimeofday( &start, NULL );
					gettimeofday( &beginTime, NULL );
					isFirst = 0; 
					startTime = 0;
				} 
				else {
					gettimeofday( &start, NULL );
					startTime = start.tv_usec + start.tv_sec*1000000 - beginTime.tv_usec - beginTime.tv_sec*1000000;
				}	
				fprintf (fp, "%010lld ", startTime); 
				
				gettimeofday( &timer, NULL );
				if( qe->responded == 0 )
				{
					qe->responded = 1;
					averageResponseTimes[qe->pid-1] += timer.tv_usec + timer.tv_sec*1000000 - qe->timeQueued;
					burstCounts[qe->pid-1] += 1;
				}	
				totalWaitingTimes[qe->pid-1] += timer.tv_sec*1000000+timer.tv_usec - qe->timeQueued;
				
				if( qe->burstTime <= quantum ) {

					fprintf (fp, "%lld ", qe->burstTime); 
					fprintf (fp, "%d\n", qe->pid);
					fflush (fp); 
					usleep(qe->burstTime*1000);
					pthread_cond_signal(&qbuffer->th_cond_burstCompleted[qe->pid-1]);
					free (qe); 
					burstTotal--;
				}
				else{
					usleep(quantum*1000);
					fprintf (fp, "%lld ", quantum); 
					fprintf (fp, "%d\n", qe->pid);
					fflush (fp); 
					qe->burstTime -= quantum;
					queue_add( qbuffer, qe);
				}
			}
			if( finishedInfiles == N ){
				break;
			}	
		}	
	}
	else
	{
		while (burstTotal != 0) {
			qe = queue_remove(qbuffer); // one thread at a time
			if( qe->pid == -1 ){
				finishedInfiles++;
			}
			else{
				long long startTime;
				if( isFirst == 1 )
				{
					gettimeofday( &start, NULL );
					gettimeofday( &beginTime, NULL );
					isFirst = 0; 
					startTime = 0;
				} 
				else {
					gettimeofday( &start, NULL );
					startTime = start.tv_usec + start.tv_sec*1000000 - beginTime.tv_usec - beginTime.tv_sec*1000000;
				}	
				fprintf (fp, "%010lld ", startTime); 
				
				gettimeofday( &timer, NULL );
				if( qe->responded == 0 )
				{
					qe->responded = 1;
					averageResponseTimes[qe->pid-1] += timer.tv_usec + timer.tv_sec*1000000 - qe->timeQueued;
					burstCounts[qe->pid-1] += 1;
				}	
				totalWaitingTimes[qe->pid-1] += timer.tv_sec*1000000+timer.tv_usec - qe->timeQueued;
				

				fprintf (fp, "%lld ", qe->burstTime); 
				fprintf (fp, "%d\n", qe->pid);
				fflush (fp);
				
				usleep(qe->burstTime*1000); 
				
				pthread_cond_signal(&qbuffer->th_cond_burstCompleted[qe->pid-1]);
				
				free (qe);
				burstTotal--;
			}
			if( finishedInfiles == N ){
				break;
			}
		}		
	}
	fclose (fp); 
	printf ("SE terminating\n"); fflush (stdout); 
	

       
       
       for( i = 0; i < N; i++){
          	pthread_join( thread[i], NULL);
       }
       
       printf("\n");
       
       for(i=0; i<N; i++)
	{	
		printf( "Total Number of CPU Bursts for Process %d is %lld\n", (i+1), burstCounts[i]  );
		averageResponseTimes[i] = averageResponseTimes[i] / burstCounts[i];
		printf( "Average response time for process %d %s %lld microsecond\n", (i+1), "is", averageResponseTimes[i]  );
		printf( "Total waiting time for process %d %s %lld microsecond\n\n", (i+1), "is", totalWaitingTimes[i]  );
	}
	
	// Destroy buffer and mutex/condition variables
	free(qbuffer->q);
	free(qbuffer);
	
	pthread_mutex_destroy(&qbuffer->th_mutex_queue);
	pthread_mutex_destroy(&qbuffer->th_mutex_process);
	pthread_cond_destroy(&qbuffer->th_cond_hasspace);
	pthread_cond_destroy(&qbuffer->th_cond_hasitem);
	for( j = 0; j < N; j++){
		pthread_cond_destroy(&qbuffer->th_cond_burstCompleted[j]);
	}
	
	
	printf ("closing...\n");
        return 0;
        
}


//Producer thread start function
void *producer(void * arg)
{	
	FILE *fp; 
	char burstName[MAXFILENAME];
	int number; 
	struct Node *qe; 
	long long M = duration;

	struct tdata* dataofthread = (struct tdata*) arg;
	
	char* indexStr; 
	
	indexStr = itoa( dataofthread->pid, 10); 
	char infilename[MAXFILENAME] = "infile";
	strcat(infilename, indexStr);
	strcat( infilename, ".txt" );
	strcpy(dataofthread->inputname, infilename); 
	
	
	
	int validInfile = strcmp(infilePrefix,"no-infile");
	
	
	if( validInfile != 0 ) {
		fp = fopen(infilename, "r"); 
		if (fp == NULL) {
		    perror("Input File Open Failed: ");
		    exit(1);
		}
		int validNumber;
		while ( fscanf(fp, "%s", burstName) == 1 ) {
			validNumber = fscanf(fp, "%d", &number);
			if( validNumber != 1 )
			{
				perror ("no CPU burst integer to read\n"); 
				exit (1);
			}
			qe = (struct Node *) malloc (sizeof (struct Node)); 
			if (qe == NULL) {
				perror ("malloc failed\n"); 
				exit (1); 
			}
			qe->next = NULL; 
			qe->burstTime = number;
			qe->pid = dataofthread->pid;
			qe->responded = 0;
			queue_add(qbuffer, qe);  // one thread at a time
			
			pthread_mutex_lock(&qbuffer->th_mutex_process);
			pthread_cond_wait(&qbuffer->th_cond_burstCompleted[dataofthread->pid-1], &qbuffer->th_mutex_process);
			pthread_mutex_unlock(&qbuffer->th_mutex_process);
			printf("Process %d proceeds to IO\n", dataofthread->pid);
			fscanf(fp, "%s", burstName);
			validNumber = fscanf(fp, "%d", &number);
			if( validNumber != 1 )
			{
				perror ("no IO Burst number to read\n"); 
				exit (1);
			}
			usleep(number*1000);
		}
		qe = (struct Node *) malloc (sizeof (struct Node)); 
		if (qe == NULL) {
			perror ("malloc failed\n"); 
			exit (1); 
		}
		qe->next = NULL; 
		qe->burstTime = BURSTMAX;
		qe->pid = -1;

		queue_add(qbuffer, qe); //add burstTime = BURSTMAX and pid = -1 to indicate end of file
		fclose (fp); 
	}
	else{
		while ( M != 0) {
			qe = (struct Node *) malloc (sizeof (struct Node)); 
			if (qe == NULL) {
				perror ("malloc failed\n"); 
				exit (1); 
			}
			qe->next = NULL; 
			qe->burstTime = randomBurst(minCPU, maxCPU);
			qe->pid = dataofthread->pid;
			qe->responded = 0;

			queue_add(qbuffer, qe); // one thread at a time
			
			pthread_mutex_lock(&qbuffer->th_mutex_process);
			pthread_cond_wait(&qbuffer->th_cond_burstCompleted[dataofthread->pid-1], &qbuffer->th_mutex_process);
			pthread_mutex_unlock(&qbuffer->th_mutex_process);
			printf("Process %d proceeds to IO\n", dataofthread->pid);
			usleep(randomBurst(minIO, maxIO)*1000);
			M--;
		}	
	}

	printf ("PS %d terminating\n", dataofthread->pid);  
	fflush (stdout); 

	pthread_exit(NULL); 
}






