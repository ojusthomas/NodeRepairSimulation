#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>

int main()
{
	int msocket,s,d,d2;
	char *q = "ssh 127.0.0.1\r\n";   
	char q2[15]="***";          
	char rdata[2000];                      //this one will save the banner grabbing from ssh server
	char rdata2[3000];                     //this one is suppose to hold the request which will be sent from server 
                                       //in order to enter the user name,BUT it's not    
	struct sockaddr_in strsock;
	msocket=socket(AF_INET,SOCK_STREAM,0);
	strsock.sin_family=AF_INET;
	strsock.sin_port=htons(22);
	strsock.sin_addr.s_addr=inet_addr("127.0.0.1");
	memset(&(strsock.sin_zero),'\0',8);

	if(connect(msocket,(struct sockaddr *)&strsock,sizeof(struct sockaddr))==-1)
	{
		perror("Connect Error");
		exit(1);
	}
	else
	{
		send(msocket,q,sizeof(q),0);
		d=recv(msocket,rdata,sizeof(rdata),0);
		send(msocket,q2,sizeof(q2),0);
		d2=recv(msocket,rdata2,sizeof(rdata2),0);
		printf("%s",rdata);
		printf("\n");
		printf("%s",rdata2);
		//printf("Connect\n");
	}
	close(msocket);
}

