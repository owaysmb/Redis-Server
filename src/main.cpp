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

using namespace std;

vector<string> RESP_parse(const string &message)
{

  vector<string> result;
  int pos = 0;

  if (message[pos] != '*')
    return result;

  pos++;

  int ElementsN = stoi(message.substr(pos, message.find("\r\n", pos) - pos));
  pos = message.find("\r\n", pos) + 2;

  for (int i = 0; i < ElementsN; i++)
  {
    if (message[pos] != '$')
      break;
    pos++;

    int newlength = stoi(message.substr(pos, message.find("\r\n", pos) - pos));
    pos = message.find("\r\n", pos) + 2;

    string word = message.substr(pos, newlength);
    result.push_back(word);

    pos += newlength + 2;
  }
  return result;
  
}

void handleCLient(int client_fd)
{
  char pingBuffer[1024];
  

  while (true)
  {
    int PingBytesRecieved = recv(client_fd, pingBuffer, sizeof(pingBuffer), 0);

    if (PingBytesRecieved <= 0)
      break;

    pingBuffer[PingBytesRecieved] = '\0';
    string message(pingBuffer);

    vector<string> cmd = RESP_parse(message);

    if (cmd.empty())
      continue;

    if (cmd[0] == "ping" || cmd[0] == "PING")
    {

      const char *response = "+PONG\r\n";
      send(client_fd, response, strlen(response), 0);
    }
    else if (cmd[0] == "ECHO" || cmd[0] == "echo"){

        if (cmd.size() > 1) {
          string reply = "$" + to_string(cmd[1].size()) + "\r\n" + cmd[1] + "\r\n";
          send(client_fd, reply.c_str(), reply.size(), 0);
      }
      
    }
  }
  close(client_fd);
}

int main(int argc, char **argv)
{

  cout << unitbuf;
  cerr << unitbuf;

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
  server_addr.sin_port = htons(6379);

  if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) != 0)
  {
    cerr << "Failed to bind to port 6379\n";
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
