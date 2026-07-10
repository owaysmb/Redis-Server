#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <thread>
#include <vector>
#include <sstream>
#include <algorithm>
#include <map>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <unordered_map>
#include <cmath>
#include <queue>
#include "Storage.h"
#include "RespParser.h"

using namespace std;

unordered_map<int, ClientState> clients;
mutex clientsMutex;

ListStorage storage;
bool isMaster = true;

void handleCommand(vector<string> &cmd, int client_fd)
{
  storage.dispatch(cmd, client_fd);
}

void handleCLient(int client_fd)
{
  {
    lock_guard<mutex> lock(clientsMutex);
    clients[client_fd] = ClientState{};
  }

  char pingBuffer[1024];
  while (true)
  {
    int PingBytesRecieved = recv(client_fd, pingBuffer, sizeof(pingBuffer), 0);

    if (PingBytesRecieved <= 0)
      break;

    pingBuffer[PingBytesRecieved] = '\0';
    string message(pingBuffer);

    vector<string> cmd = RESP_parse(message);

    handleCommand(cmd, client_fd);
  }
  close(client_fd);
  {
    lock_guard<mutex> lock(clientsMutex);
    clients.erase(client_fd);
  }
}

int main(int argc, char *argv[])
{

  cout << unitbuf;
  cerr << unitbuf;
  string port = "6379";
  string masterHost = "";
  string masterPort = "";

  if(argc == 5 ){
    masterHost = argv[3];
    masterPort = argv[4];
  }

  for (int i = 1; i < argc; i++)
  {
    
    if (string(argv[i]) == "--port" && i + 1 < argc)
    {
      port = argv[i + 1];
    }else if (string(argv[i]) == "--replicaof") {
        isMaster = false;
    }

  }

  int server_fd = socket(AF_INET, SOCK_STREAM, 0);

  if (server_fd < 0)
  {
    cerr << "Failed to create server socket\n";
    return 1;
  }

  int reuse = 1;

  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0)
  {
    cerr << "setsockopt failed\n";
    return 1;
  }

  struct sockaddr_in server_addr;

  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(stoi(masterPort));

  if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) != 0)
  {
    cerr << "Failed to bind to port " << port << "\n";
    return 1;
  }

  int connection_backlog = 5;

  if (listen(server_fd, connection_backlog) != 0)
  {
    cerr << "listen failed\n";
    return 1;
  }

  struct sockaddr_in client_addr;
  int client_addr_len = sizeof(client_addr);
  cout << "Waiting for a client to connect...\n";

  cout << "Logs from your program will appear here!\n";

  cout << "Client connected\n";

  while (true)
  {
    int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, (socklen_t *)&client_addr_len);
    thread t(handleCLient, client_fd);
    t.detach();
  }

  close(server_fd);

  return 0;
}
