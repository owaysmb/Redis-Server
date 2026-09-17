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
extern long long replicationOffset;

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

string readReply(int sock_fd)
{
  char buf[1024];
  char pingBuffer[1024];

  int bytesRead = recv(sock_fd, buf, sizeof(buf) - 1, 0);
  if (bytesRead <= 0)
  {
    return "";
  }

  pingBuffer[bytesRead] = '\0';
  string message(pingBuffer);

  vector<string> cmd = RESP_parse(message);

  handleCommand(cmd, sock_fd);

  buf[bytesRead] = '\0';
  return string(buf);
}
string readLine(int sock_fd, string &buffer)
{
  size_t pos;
  while ((pos = buffer.find("\r\n")) == string::npos)
  {
    char tmp[4096];
    int n = recv(sock_fd, tmp, sizeof(tmp), 0);
    if (n <= 0)
      return "";
    buffer.append(tmp, n);
  }
  string line = buffer.substr(0, pos);
  buffer.erase(0, pos + 2);
  return line;
}

void readExact(int sock_fd, string &buffer, size_t n)
{
  while (buffer.size() < n)
  {
    char tmp[4096];
    int r = recv(sock_fd, tmp, sizeof(tmp), 0);
    if (r <= 0)
      return;
    buffer.append(tmp, r);
  }
}
void connectToMaster(const string &masterHost, const string &masterPort, const string &myPort)
{
  int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (sock_fd < 0)
  {
    cerr << "Failed to create socket to master\n";
    return;
  }

  struct sockaddr_in master_addr;
  master_addr.sin_family = AF_INET;
  master_addr.sin_port = htons(stoi(masterPort));

  struct hostent *he = gethostbyname(masterHost.c_str());
  if (he == nullptr)
  {
    cerr << "Failed to resolve master host: " << masterHost << "\n";
    close(sock_fd);
    return;
  }
  memcpy(&master_addr.sin_addr, he->h_addr_list[0], he->h_length);

  if (connect(sock_fd, (struct sockaddr *)&master_addr, sizeof(master_addr)) < 0)
  {
    cerr << "Failed to connect to master\n";
    close(sock_fd);
    return;
  }

  masterConnectionFd = sock_fd;

  string ping = "*1\r\n$4\r\nPING\r\n";
  send(sock_fd, ping.c_str(), ping.size(), 0);
  readReply(sock_fd);

  string portStr = myPort;
  string replconf1 = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" + to_string(portStr.size()) + "\r\n" + portStr + "\r\n";
  send(sock_fd, replconf1.c_str(), replconf1.size(), 0);
  readReply(sock_fd);

  string replconf2 = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
  send(sock_fd, replconf2.c_str(), replconf2.size(), 0);
  readReply(sock_fd);

  string psync = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
  send(sock_fd, psync.c_str(), psync.size(), 0);

  string leftover;

  string fullresyncLine = readLine(sock_fd, leftover);

  string rdbHeader = readLine(sock_fd, leftover);
  int rdbLen = stoi(rdbHeader.substr(1));

  readExact(sock_fd, leftover, rdbLen);
  leftover.erase(0, rdbLen);

  char recvBuf[4096];
  while (true)
  {
    size_t pos = 0;
    while (true)
    {
      size_t before = pos;
      vector<string> cmd = RESP_parse_one(leftover, pos);
      if (cmd.empty() && pos == before)
        break;

      handleCommand(cmd, sock_fd);
      replicationOffset += pos - before;
    }
    leftover = leftover.substr(pos);

    int bytesReceived = recv(sock_fd, recvBuf, sizeof(recvBuf), 0);
    if (bytesReceived <= 0)
      break;
    leftover.append(recvBuf, bytesReceived);
  }
}

int main(int argc, char *argv[])
{
  cout << unitbuf;
  cerr << unitbuf;
  string port = "6379";
  string masterHost, masterPort;

  for (int i = 1; i < argc; i++)
  {
    if (string(argv[i]) == "--port" && i + 1 < argc)
    {
      port = argv[i + 1];
    }
    else if (string(argv[i]) == "--replicaof" && i + 1 < argc)
    {
      isMaster = false;
      string replicaofArg = argv[i + 1];
      size_t spacePos = replicaofArg.find(' ');
      masterHost = replicaofArg.substr(0, spacePos);
      masterPort = replicaofArg.substr(spacePos + 1);
      i++;
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
  server_addr.sin_port = htons(stoi(port));

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

  if (!isMaster)
  {
    thread(connectToMaster, masterHost, masterPort, port).detach();
  }

  struct sockaddr_in client_addr;
  int client_addr_len = sizeof(client_addr);
  cout << "Waiting for a client to connect...\n";
  cout << "Logs from your program will appear here!\n";

  while (true)
  {
    int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, (socklen_t *)&client_addr_len);
    thread t(handleCLient, client_fd);
    t.detach();
  }

  close(server_fd);
  return 0;
}