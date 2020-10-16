#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <string.h>
#include <ctype.h>
#include <sys/time.h>
#include <pthread.h>
#include "fs/operations.h"

#define MAX_COMMANDS 150000
#define MAX_INPUT_SIZE 100

int numberThreads = 0;
char inputCommands[MAX_COMMANDS][MAX_INPUT_SIZE];
int numberCommands = 0;
int headQueue = 0;

// Variaveis das threads
pthread_mutex_t mutex_fs;
pthread_mutex_t mutex_comandos;
pthread_rwlock_t rwlock;
int lockOption = 0;

int insertCommand(char* data) {
    if(numberCommands != MAX_COMMANDS) {
        strcpy(inputCommands[numberCommands++], data);
        return 1;
    }
    return 0;
}

char* removeCommand() {
    pthread_mutex_lock(&mutex_comandos); // Limitar o uso do vetor de comandos para uma thread de cada vez
    if(numberCommands > 0){
        numberCommands--;
        return inputCommands[headQueue++];  
    }
    pthread_mutex_unlock(&mutex_comandos); // Fazer Unlock imediatamente quando nao ha mais comandos a ser executados
    return NULL;
}

void errorParse(){
    fprintf(stderr, "Error: command invalid\n");
    exit(EXIT_FAILURE);
}

void processInput(FILE *f){
    char line[MAX_INPUT_SIZE];

    /* break loop with ^Z or ^D */
    while (fgets(line, sizeof(line)/sizeof(char), f)) {
        char token, type;
        char name[MAX_INPUT_SIZE];

        int numTokens = sscanf(line, "%c %s %c", &token, name, &type);

        /* perform minimal validation */
        if (numTokens < 1) {
            continue;
        }
        switch (token) {           
            case 'c':
                if(numTokens != 3)
                    errorParse();
                if(insertCommand(line))
                    break;
                return;
            
            case 'l':
                if(numTokens != 2)
                    errorParse();
                if(insertCommand(line))
                    break;
                return;
            
            case 'd':
                if(numTokens != 2)
                    errorParse();
                if(insertCommand(line))                  
                    break;
                return;
            
            case '#':
                break;
            
            default: { /* error */
                errorParse();
            }
        }
    }
}

void *applyCommands(){
    int numTokens, searchResult;
    char token, type;
    char name[MAX_INPUT_SIZE];
	
    while(numberCommands > 0){
        
        const char* command = removeCommand(); //Secção Crítica
        if (command == NULL){
            continue;
        }       
        numTokens = sscanf(command, "%c %s %c", &token, name, &type);
        if (numTokens < 2) {
            fprintf(stderr, "Error: invalid command in Queue\n");
            exit(EXIT_FAILURE);
        }

        switch (token) {
            case 'c':
                switch (type) {
                    case 'f':
                        if(lockOption == 1) {pthread_mutex_lock(&mutex_fs);}
                        else if(lockOption == 2) {pthread_rwlock_wrlock(&rwlock);}
                        pthread_mutex_unlock(&mutex_comandos);
                        
                        printf("Create file: %s\n", name);
                        create(name, T_FILE); //Secção Crítica

                        if(lockOption == 1) {pthread_mutex_unlock(&mutex_fs);}
                        else if(lockOption == 2) {pthread_rwlock_unlock(&rwlock);}
                        break;
                    case 'd':
                        if(lockOption == 1) {pthread_mutex_lock(&mutex_fs);}
                        else if(lockOption == 2) {pthread_rwlock_wrlock(&rwlock);}
                        pthread_mutex_unlock(&mutex_comandos);
                        
                        printf("Create directory: %s\n", name);
                        create(name, T_DIRECTORY); //Secção Crítica

                        if(lockOption == 1) {pthread_mutex_unlock(&mutex_fs);}
                        else if(lockOption == 2) {pthread_rwlock_unlock(&rwlock);}
                        break;
                    default:
                        fprintf(stderr, "Error: invalid node type\n");
                        exit(EXIT_FAILURE);
                }
                break;
            case 'l': 
                if(lockOption == 1) {pthread_mutex_lock(&mutex_fs);}
                else if(lockOption == 2) {pthread_rwlock_rdlock(&rwlock);}
                pthread_mutex_unlock(&mutex_comandos);
                
                searchResult = lookup(name); //Secção Crítica
                if (searchResult >= 0)
                    printf("Search: %s found\n", name);
                else
                    printf("Search: %s not found\n", name);
                
                if(lockOption == 1) {pthread_mutex_unlock(&mutex_fs);}
                else if(lockOption == 2) {pthread_rwlock_unlock(&rwlock);}
                break;
            case 'd':
                if(lockOption == 1) {pthread_mutex_lock(&mutex_fs);}
                else if(lockOption == 2) {pthread_rwlock_wrlock(&rwlock);}
                pthread_mutex_unlock(&mutex_comandos);
                
                printf("Delete: %s\n", name);
                delete(name); //Secção Crítica

                if(lockOption == 1) {pthread_mutex_unlock(&mutex_fs);}
                else if(lockOption == 2) {pthread_rwlock_unlock(&rwlock);}
                break;
            default: { /* error */
                fprintf(stderr, "Error: command to apply\n");
                exit(EXIT_FAILURE);
            }
        }
    }
    if (lockOption != 0) {
        pthread_exit(NULL);
    }
    return NULL;
}

int main(int argc, char* argv[]) {
    FILE *flptr;
    struct timeval start, end;
    numberThreads = atoi(argv[3]);
    

    // init filesystem
    init_fs();

    // process input
    flptr = fopen(argv[1], "r");
    processInput(flptr);
    fclose(flptr);

    
	
	// Inicializacao das locks
	if(strcmp(argv[4], "mutex") == 0) {
        lockOption = 1;
        pthread_mutex_init(&mutex_fs, NULL);
    } else if (strcmp(argv[4], "rwlock") == 0) {
        lockOption = 2;
        pthread_rwlock_init(&rwlock, NULL);
    }

    // Execucao dos comandos
    if(lockOption == 0) {
        if (numberThreads == 1) {
            gettimeofday(&start, NULL);
            applyCommands();
            gettimeofday(&end, NULL);
        } else {
            fprintf(stderr, "Error: Please use a different lock strategy\n");
            exit(EXIT_FAILURE);
        }
    } else {
        if (numberThreads == 0) {
            fprintf(stderr, "Error: Please use a thread number higher than 0 for this specific lock strategy\n");
            exit(EXIT_FAILURE);
        }
        pthread_t tid[numberThreads]; // Declaração das Threads de acordo com o argumento recebido

        if (pthread_mutex_init(&mutex_comandos, NULL) != 0) {
            fprintf(stderr, "Error: Couldn't initialize mutex\n");
            exit(EXIT_FAILURE);
        }

        if (lockOption == 1) {
            if (pthread_mutex_init(&mutex_fs, NULL) != 0) {
                fprintf(stderr, "Error: Couldn't initialize mutex\n");
                exit(EXIT_FAILURE);
            } 
        } else {
            if (pthread_rwlock_init(&rwlock, NULL) != 0) {
                fprintf(stderr, "Error: Couldn't initialize rwlock\n");
                exit(EXIT_FAILURE);
            }
        }

        gettimeofday(&start, NULL);
        for (int i = 0; i < numberThreads; i++){
            int thr_status = pthread_create(&tid[i], NULL, applyCommands, NULL);
            if (thr_status != 0){
                fprintf(stderr, "Error: Couldn't create thread\n");
                exit(EXIT_FAILURE);
            }
        }

        for (int i = 0; i < numberThreads; i++){
            int thr_status = pthread_join(tid[i], NULL);
            if (thr_status != 0){
                fprintf(stderr, "Error: Couldn't join thread\n");
                exit(EXIT_FAILURE);
            }
        }
        gettimeofday(&end, NULL);
    }
    
	
	// Destruir as locks depois das threads acabarem
	if(lockOption == 1) {
        int err = pthread_mutex_destroy(&mutex_fs);
        if (err != 0) {
            fprintf(stderr, "Error: Couldn't destroy mutex\n");
            exit(EXIT_FAILURE);
        }
    } else if (lockOption == 2) {
        int err = pthread_rwlock_destroy(&rwlock);
        if (err != 0) {
            fprintf(stderr, "Error: Couldn't destroy rwlock\n");
            exit(EXIT_FAILURE);
        }
    }
    int err = pthread_mutex_destroy(&mutex_comandos);
    if (err != 0) {
        fprintf(stderr, "Error: Couldn't destroy mutex\n");
        exit(EXIT_FAILURE);
    }

    // Print do tempo de execucao
    printf("TecnicoFS completed in %.4f seconds.\n", ((double)(end.tv_usec - start.tv_usec) / 100000) + (double)(end.tv_sec - start.tv_sec));

    //Print da arvore para o ficheiro
    flptr = fopen(argv[2], "w");
    print_tecnicofs_tree(flptr);
    fclose(flptr);

    // release allocated memory
    destroy_fs();
    exit(EXIT_SUCCESS);
}
