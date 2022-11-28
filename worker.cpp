#include "CurlEasyPtr.h"
#include <cstdio>
#include <iostream>
#include <sstream>
#include <string>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstring>
// TODO: remove sleep
#include<unistd.h>               // for linux 

using namespace std::literals;

/// Worker process that receives a list of URLs and reports the result
/// Example:
///    ./worker localhost 4242
/// The worker then contacts the leader process on "localhost" port "4242" for work

size_t processFile(std::string url) {
   size_t result = 0;
   auto curlSetup = CurlGlobalSetup();
   auto curl = CurlEasyPtr::easyInit();

   curl.setUrl(url);
   auto csvData = curl.performToStringStream();

   for (std::string row; std::getline(csvData, row, '\n');) {
      auto rowStream = std::stringstream(std::move(row));

      // Check the URL in the second column
      unsigned columnIndex = 0;
      for (std::string column; std::getline(rowStream, column, '\t'); ++columnIndex) {
         // column 0 is id, 1 is URL
         if (columnIndex == 1) {
            // Check if URL is "google.ru"
            auto pos = column.find("://"sv);
            if (pos != std::string::npos) {
               auto afterProtocol = std::string_view(column).substr(pos + 3);
               if (afterProtocol.starts_with("google.ru/")) {
                  ++result;
               }
            }
            break;
         }
      }
   }
   return result;
}

int main(int argc, char* argv[]) {
   // TODO: fix connect and remove sleep
   sleep(1);   
   
   std::cout << "Worker started!" << std::endl;

   if (argc != 3) {
      std::cerr << "Usage: " << argv[0] << " <host> <port>" << std::endl;
      return 1;
   }

   // 1. connect to coordinator specified by host and port getaddrinfo(), connect(), see: https://beej.us/guide/bgnet/html/#system-calls-or-bust
   // 1.1. getaddrinfo()
   const size_t bufferSize = 2048;
   const char* hostName = argv[1];
   const char* portNumber = argv[2];
   int clientSocket;
   struct addrinfo hints;
   memset(&hints, 0, sizeof(struct addrinfo));
   struct addrinfo* results;
   struct addrinfo* record;
   hints.ai_family = AF_INET;
   hints.ai_socktype = SOCK_STREAM;
   hints.ai_protocol = IPPROTO_TCP;

   if ((getaddrinfo(hostName, portNumber, &hints, &results)) != 0) { // Translate address
      std::perror("Failed to translate worker socket.");
      exit(EXIT_FAILURE);
   }
   std::cout << "Worker socket translated." << std::endl;

   // 1.2. connect()
   for (record = results; record != NULL; record = record->ai_next) { // Iterate through every record in results
      clientSocket = socket(record->ai_family, record->ai_socktype, record->ai_protocol); // Attempt to create socket from information provided in current record
      // Skip current iteration in the loop if socket creation fails
      if (clientSocket == -1) {
         continue;
      }
      // Attempt to connect socket after its successful creation. If successful, break out of the loop
      if (connect(clientSocket, record->ai_addr, record->ai_addrlen) != -1) {
         break;
      }
      // Close the socket if socket creation is successful but connection is unsuccessful
      close(clientSocket);
   }
   // record will iterate to NULL if the above loop encounters no success
   if (record == NULL) {
      std::perror("Failed to create or connect worker socket.");
      exit(EXIT_FAILURE);
   }
   freeaddrinfo(results);
   std::cout << "Worker socket created and connected." << std::endl;

   // 2. receive work from coordinator recv(), matching the coordinator's send() work
   // 2.1. recv()
   // int noWork = false;
   int job_received = 0;
   while (true) {
      char buffer[bufferSize];
      size_t found = 0;
      size_t urlLen;
      ssize_t nbytes=recv(clientSocket, buffer, sizeof(urlLen), 0);
      if (nbytes < 1) { // Read message
         perror("Failed to receive message.");
         exit(EXIT_FAILURE);
      } else if (nbytes == 0) {
         // server closed
         std::cout << "Worker: server socket closed." << std::endl;
         sleep(15);
         // TODO: should change to break, but not working
         std::cout << "Worker completes:" << job_received <<" jobs." << std::endl;
         continue;
      }
      urlLen = static_cast<size_t> (*buffer);
      ssize_t receive;
      std::cout << "urlLen: " << urlLen << std::endl;
      if ((receive = recv(clientSocket, buffer, urlLen, MSG_WAITALL)) == -1) { // Read message
         perror("Failed to receive message.");
         exit(EXIT_FAILURE);
      }
      if (urlLen != (size_t) receive) {
         std::cout << "Didn't receive enough data, only" << receive << "bytes." << std::endl;
         continue;
      }

      //    3. process work see coordinator.cpp
      //    3.1. process work
      std::cout << "Worker received message: " << buffer << std::endl;
      std::string file(buffer);
      job_received ++;
      file = file.substr(0, 108);
      
      found = processFile(file);

      // 4. report result send(), matching the coordinator's recv()
      if (send(clientSocket, &found, sizeof(found), 0) == -1) { // Send data
         std::perror("Failed to perform cognitive recalibration."); // Error message for when send() fails
         exit(EXIT_FAILURE);
      } else {
         std::cout << "Worker sent" << found << std::endl;
      }
   }

   // 6. close connection close()
   close(clientSocket);

   std::cout << "Worker socket closed." << std::endl;

   return 0;
}
