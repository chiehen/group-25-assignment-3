#include "CurlEasyPtr.h"
#include <cstring>
#include <deque>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace std::literals;

// Add a new file descriptor to the set
void add_to_pfds(struct pollfd* pfds[], int newfd, size_t* fd_count, size_t* fd_size) {
   // If we don't have room, add more space in the pfds array
   if (*fd_count == *fd_size) {
      *fd_size *= 2; // Double it

      *pfds = (pollfd*) realloc(*pfds, sizeof(**pfds) * (*fd_size));
   }

   (*pfds)[*fd_count].fd = newfd;
   (*pfds)[*fd_count].events = POLLIN | POLLOUT; // Check ready-to-read, ready-to-send

   (*fd_count)++;
}

// Remove an index from the set
void del_from_pfds(struct pollfd pfds[], int i, size_t* fd_count) {
   // Copy the one from the end over this one
   pfds[i] = pfds[*fd_count - 1];

   (*fd_count)--;
}

/// Leader process that coordinates workers. Workers connect on the specified port
/// and the coordinator distributes the work of the CSV file list.
/// Example:
///    ./coordinator http://example.org/filelist.csv 4242
int main(int argc, char* argv[]) {
   // std::cout << "Coordinator started." << std::endl;

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
   // std::cout << "Coordinator socket translated." << std::endl;

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
   // std::cout << "Server socket created and bound." << std::endl;

   /// 1.4. listen()
   if (listen(serverSocket, backlog) == -1) { // Start server socket listen
      std::perror("Failed to start server socket listen.");
      exit(EXIT_FAILURE);
   }
   // std::cout << "Server started listening." << std::endl;

   auto curlSetup = CurlGlobalSetup();

   auto listUrl = std::string(argv[1]);

   // Download the file list
   auto curl = CurlEasyPtr::easyInit();
   curl.setUrl(listUrl);
   auto fileList = curl.performToStringStream();

   //TODO: change to list
   // track un-assigned urls
   std::deque<std::string> jobs;

   // std::cout << "Jobs:" << std::endl;
   for (std::string url; std::getline(fileList, url, '\n');) {
      jobs.push_back(url);
   }

   /// 2. Distribute the following work among workers send() them some work
   
   // std::cout << "Coordinator still running" << std::endl;
   // Start off with room for 5 connections
   // (We'll realloc as necessary)
   size_t fd_count = 0;
   size_t fd_size = 5;
   struct pollfd* pfds = (pollfd*) malloc(sizeof *pfds * fd_size);

   // <fd, job_list>
   std::map<int, std::deque<std::string>> jobMap;
   // <fd, job_count>
   std::map<int, int> busyWorker;

   // Add the serverSocket to set
   pfds[0].fd = serverSocket;
   pfds[0].events = POLLIN; // Report ready to read on incoming connection

   size_t sum = 0;

   fd_count = 1; // For the serverSocket
   while (true) {  
      // std::cout << "Server started pollling." << std::endl;
      if (busyWorker.empty() && jobs.empty()) {
         // std::cout << "Server completed the jobs." << std::endl;
         break;
      }

      int poll_count = poll(pfds, static_cast<nfds_t>(fd_count), -1);
      if (poll_count == -1) {
         perror("poll");
         exit(1);
      }

      for (size_t i = 0; i < fd_count; i++) {
         int fd = pfds[i].fd;
         if (pfds[i].revents & (POLLHUP | POLLERR | POLLNVAL)) {
               /// 3.2 handle failed node
               while (!jobMap[fd].empty()) {
                  std::string url = jobMap[fd].front();
                  jobs.push_back(url);
                  jobMap[fd].pop_front();
               }
               jobMap.erase(fd);
               busyWorker.erase(fd);
               del_from_pfds(pfds, (int)i, &fd_count);
               continue;
            }
         if (pfds[i].revents & (POLLIN | POLLOUT)) {
            // std::cout << "Server work on fd:\t" << fd << std::endl;
            if (pfds[i].fd == serverSocket && (pfds[i].revents & POLLIN) ) {
               // std::cout << "SeverSocket:\t" << (pfds[i].revents & POLLIN) << std::endl;
               // 2.1 
               // new incoing connection
               int clientSocket;
               struct sockaddr clientAddress;
               socklen_t clientAddressLength = sizeof(clientAddress);
               if ((clientSocket = accept(serverSocket, &clientAddress, &clientAddressLength)) < 0) {
                  std::perror("Failed to accept client socket.");
                  exit(EXIT_FAILURE);
               }
               // std::cout << "Socket:\t" << clientSocket << " Client socket accepted." << std::endl;
               add_to_pfds(&pfds, clientSocket, &fd_count, &fd_size);
               // to have more connections at once
               sleep(1);
               break;
            } else {
               // regular worker
               if (pfds[i].revents & POLLOUT) {
                  /// 2.3. send() when queue is not empty
                  // TODO: check queue is not empty
                  if (!jobs.empty()) {
                     std::string url = jobs.front();
                     jobs.pop_front();
                     size_t urlLen = strlen(url.c_str());
                     // std::cout << "Server send Message: " << url << std::endl;
                     if (send(pfds[i].fd, &urlLen, sizeof(urlLen), 0) < 0) {
                        std::perror("Failed to send message to client.");
                        exit(EXIT_FAILURE);
                     }
                     if (send(pfds[i].fd, url.c_str(), urlLen, 0) < 0) {
                        std::perror("Failed to send message to client.");
                        exit(EXIT_FAILURE);
                     }
                     /// 2.4 add to JobMap & busyWorker
                     if (jobMap.count(fd) > 0) {
                        jobMap[fd].push_back(url);
                        busyWorker[fd]++;
                     } else {
                        std::deque<std::string> workerJob = {url};
                        jobMap.insert({fd, workerJob});
                        busyWorker.insert({fd, 1});
                     }
                  }
               }
               if (pfds[i].revents & POLLIN) {
                  /// 3. Collect all results recv() the results
                  int buffer[1024];
                  ssize_t nbytes = recv(pfds[i].fd, &buffer, sizeof(sum), 0);
                  if (nbytes == -1) {
                     perror("Failed to receive message.");
                     exit(EXIT_FAILURE);
                  } else if (nbytes == 0) {
                     continue;
                  } else {
                     /// 3.1 Add result from client to sum
                     // std::cout << "Server: Message received: " << *buffer << std::endl;
                     // parse buffer
                     jobMap[fd].pop_front();
                     busyWorker[fd]--;
                     if (busyWorker[fd] == 0) {
                        jobMap.erase(fd);
                        busyWorker.erase(fd);
                     }
                     sum += static_cast<size_t>(*buffer);
                     // std::cout << "sum: " << sum << std::endl;
                  }
               }
            }
         }
      }
   }
   /// 4. Close the socket close()
   for (size_t i = 0; i < fd_count; i++) {
      close(pfds[i].fd);
   }
   free(pfds);
   close(serverSocket);
   // std::cout << "Coordinator finished." << std::endl;
   std::cout << sum << std::endl;
   return 0;
}
