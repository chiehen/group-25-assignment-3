#include "CurlEasyPtr.h"
#include <iostream>
#include <sstream>
#include <string>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace std::literals;

/// Leader process that coordinates workers. Workers connect on the specified port
/// and the coordinator distributes the work of the CSV file list.
/// Example:
///    ./coordinator http://example.org/filelist.csv 4242
int main(int argc, char* argv[]) {
   std::cout << "Coordinator started." << std::endl;

   if (argc != 3) {
      std::cerr << "Usage: " << argv[0] << " <URL to csv list> <listen port>" << std::endl;
      return 1;
   }

   /// 1. Allow workers to connect socket(), bind(), listen(), accept(), see: https://beej.us/guide/bgnet/html/#system-calls-or-bust
   /// 1.1. getaddrinfo()
   const char* portNumber = argv[2];
   const int backlog = 1;
   int serverSocket;
   struct addrinfo hints;
   memset(&hints, 0, sizeof(struct addrinfo));
   struct addrinfo* results;
   struct addrinfo* record;
   hints.ai_family = AF_INET;
   hints.ai_socktype = SOCK_STREAM;
   hints.ai_protocol = IPPROTO_TCP;

   if ((getaddrinfo(NULL, portNumber, &hints, &results)) != 0) { // Translate address
      std::perror("Failed to translate coordinator socket.");
      exit(EXIT_FAILURE);
   }
   std::cout << "Coordinator socket translated." << std::endl;

   /// 1.2. socket()
   for (record = results; record != NULL; record = record->ai_next) {
      serverSocket = socket(record->ai_family, record->ai_socktype, record->ai_protocol); // Attempt to create socket from information provided in current record
      // Skip current iteration in the loop if socket creation failss
      if (serverSocket == -1) {
         continue;
      }
      int enable = 1;
      setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)); // Configure server socket to reuse same port. Helpful if the server needs to restart
      /// 1.3. bind()
      if (bind(serverSocket, record->ai_addr, record->ai_addrlen) == 0) {
         break;
      }
      // Close the socket if socket creation is successful but binding is unsuccessful
      close(serverSocket);
   }

   if (record == NULL) { // record will iterate to NULL if the above loop encounters no success
      std::perror("Failed to create or connect worker socket.");
      exit(EXIT_FAILURE);
   }
   freeaddrinfo(results);
   std::cout << "Server socket created and bound." << std::endl;

   /// 1.4. listen()
   if (listen(serverSocket, backlog) == -1) { // Start server socket listen
      std::perror("Failed to start server socket listen.");
      exit(EXIT_FAILURE);
   }
   std::cout << "Server started listening." << std::endl;

   /// 2. Distribute the following work among workers send() them some work
   while (true) {
      std::cout << "Coordinator still running" << std::endl;
      /// 2.1. accept()
      int clientSocket;
      struct sockaddr clientAddress;
      socklen_t clientAddressLength = sizeof(clientAddress);

      if ((clientSocket = accept(serverSocket, &clientAddress, &clientAddressLength)) < 0) {
         std::perror("Failed to accept client socket.");
         exit(EXIT_FAILURE);
      }
      std::cout << "Socket:\t" << clientSocket << " Client socket accepted." << std::endl;
      /// 2.2. find the next work item
      auto curlSetup = CurlGlobalSetup();

      auto listUrl = std::string(argv[1]);

      // Download the file list
      auto curl = CurlEasyPtr::easyInit();
      curl.setUrl(listUrl);
      auto fileList = curl.performToStringStream();

      size_t sum = 0;
      // Iterate over all files
      for (std::string url; std::getline(fileList, url, '\n');) {
         /// 2.3. send()
         if (send(clientSocket, url.c_str(), strlen(url.c_str()), 0) < 0) {
            std::perror("Failed to send message to client.");
            exit(EXIT_FAILURE);
         }
         /// 3. Collect all results recv() the results
         int buffer[1024];
         if (recv(clientSocket, &buffer, sizeof(buffer), 0) == -1) { // Read message
            perror("Failed to receive message.");
            exit(EXIT_FAILURE);
         }
         std::cout << "Message received: " << *buffer << std::endl;
         std::cout << "Message received: " << strlen("file:///Users/konstantinosalexoudis/Code/src/gitlab.lrz.de/kalexoudis/group-25-assignment-3/data/test.00.csv") << std::endl;
         sum += static_cast<unsigned long>(*buffer);
      }
      std::cout << sum << std::endl;
      break;
   }
   /// 4. Close the socket close()
   close(serverSocket);
   std::cout << "Coordinator finished." << std::endl;
   return 0;
}
// Hint: Think about how you track which worker got what work
/*
   auto curlSetup = CurlGlobalSetup();

   auto listUrl = std::string(argv[1]);

   // Download the file list
   auto curl = CurlEasyPtr::easyInit();
   curl.setUrl(listUrl);
   auto fileList = curl.performToStringStream();

   size_t result = 0;
   // Iterate over all files
   for (std::string url; std::getline(fileList, url, '\n');) {
      curl.setUrl(url);
      // Download them
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
                  if (afterProtocol.starts_with("google.ru/"))
                     ++result;
               }
               break;
            }
         }
      }
   }
   std::cout << result << std::endl;
   */
