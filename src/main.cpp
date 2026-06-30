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

class ListStorage
{
private:
  unordered_map<string, chrono::steady_clock::time_point> ExpiryTimes;
  unordered_map<string, string> Database;
  unordered_map<string, vector<string>> List;
  map<string, vector<pair<string, map<string, string>>>> Streams;
  mutex mtx;
  condition_variable cv;

public:
  void handlePing(vector<string> &cmd, int client_fd)
  {
    const char *response = "+PONG\r\n";
    send(client_fd, response, strlen(response), 0);
  }

  void handleEcho(vector<string> &cmd, int client_fd)
  {
    if (cmd.size() > 1)
    {
      string reply = "$" + to_string(cmd[1].size()) + "\r\n" + cmd[1] + "\r\n";
      send(client_fd, reply.c_str(), reply.size(), 0);
    }
  }

  void handleSET(vector<string> &cmd, int client_fd)
  {
    if (cmd.size() < 3)
      return;

    string key = cmd[1];
    string value = cmd[2];
    ExpiryTimes.erase(key);

    if (cmd.size() >= 5)
    {
      if (cmd[3] == "EX" || cmd[3] == "ex")
      {
        long seconds = stol(cmd[4]);
        ExpiryTimes[key] = chrono::steady_clock::now() + chrono::seconds(seconds);
      }
      else if (cmd[3] == "PX" || cmd[3] == "px")
      {
        long millis = stol(cmd[4]);
        ExpiryTimes[key] = chrono::steady_clock::now() + chrono::milliseconds(millis);
      }
    }

    Database[cmd[1]] = cmd[2];
    const char *response = "+OK\r\n";
    send(client_fd, response, strlen(response), 0);
  }

  void handleGET(vector<string> &cmd, int client_fd)
  {
    if (cmd.size() < 2)
      return;

    string key = cmd[1];
    auto expiryIt = ExpiryTimes.find(key);
    if (expiryIt != ExpiryTimes.end() && chrono::steady_clock::now() >= expiryIt->second)
    {
      Database.erase(key);
      ExpiryTimes.erase(key);
    }

    auto it = Database.find(key);
    if (it != Database.end())
    {
      string value = it->second;
      string reply = "$" + to_string(value.size()) + "\r\n" + value + "\r\n";
      send(client_fd, reply.c_str(), reply.size(), 0);
    }
    else
    {
      const char *nullReply = "$-1\r\n";
      send(client_fd, nullReply, strlen(nullReply), 0);
    }
  }

  void handleRPUSH(vector<string> &cmd, int client_fd)

  {

    if (cmd.size() < 3)
      return;

    string key = cmd[1];

    for (int i = 2; i < cmd.size(); i++)
    {
      string value = cmd[i];
      List[key].push_back(value);
      cv.notify_one();
    }

    int response = List[key].size();

    string reply = ":" + to_string(response) + "\r\n";
    send(client_fd, reply.c_str(), reply.size(), 0);
  }

  void handleLPUSH(vector<string> &cmd, int client_fd)
  {

    if (cmd.size() < 3)
      return;

    string key = cmd[1];

    for (int i = 2; i < cmd.size(); i++)
    {
      string value = cmd[i];
      List[key].insert(List[key].begin(), value);
      cv.notify_one();
    }

    int response = List[key].size();

    string reply = ":" + to_string(response) + "\r\n";
    send(client_fd, reply.c_str(), reply.size(), 0);
  }

  void handleLRANGE(vector<string> &cmd, int client_fd)
  {
    if (cmd.size() < 4)
      return;

    string key = cmd[1];

    auto it = List.find(key);
    if (it == List.end())
    {
      const char *emptyArray = "*0\r\n";
      send(client_fd, emptyArray, strlen(emptyArray), 0);
      return;
    }

    vector<string> &items = it->second;
    int start = stoi(cmd[2]);
    int stop = stoi(cmd[3]);

    if (start < 0)
      start = items.size() + start;
    if (stop < 0)
      stop = items.size() + stop;
    if (stop >= (int)items.size())
      stop = items.size() - 1;

    if (start > stop || items.empty())
    {
      const char *emptyArray = "*0\r\n";
      send(client_fd, emptyArray, strlen(emptyArray), 0);
      return;
    }
    if (start < 0)
      start = 0;
    vector<string> result;

    for (int i = start; i <= stop; i++)
    {
      result.push_back(items[i]);
    }

    string reply = "*" + to_string(result.size()) + "\r\n";
    for (auto &val : result)
    {
      reply += "$" + to_string(val.size()) + "\r\n" + val + "\r\n";
    }

    send(client_fd, reply.c_str(), reply.size(), 0);
  }

  void handleLLEN(vector<string> &cmd, int client_fd)
  {

    if (cmd.size() < 2)
      return;

    string key = cmd[1];
    auto it = List.find(key);

    int length = 0;

    if (it != List.end())
    {
      length = it->second.size();
    }
    string reply = ":" + to_string(length) + "\r\n";
    send(client_fd, reply.c_str(), reply.size(), 0);
  }

  void handleLPOP(vector<string> &cmd, int client_fd)
  {
    if (cmd.size() < 2)
      return;

    string key = cmd[1];
    string result = "";
    auto it = List.find(key);

    if (cmd.size() == 3 && it != List.end())
    {
      string elements = cmd[2];
      vector<string> result2;

      for (int i = 0; i < stoi(elements); i++)
      {
        result2.push_back(List[key][0]);
        List[key].erase(List[key].begin());
      }

      string reply = "*" + to_string(result2.size()) + "\r\n";
      for (auto &val : result2)
      {
        reply += "$" + to_string(val.size()) + "\r\n" + val + "\r\n";
      }

      send(client_fd, reply.c_str(), reply.size(), 0);
      return;
    }

    if (it != List.end() && cmd.size() == 2)
    {
      result = List[key][0];
      List[key].erase(List[key].begin());
    }

    string reply = "$" + to_string(result.size()) + "\r\n" + result + "\r\n";
    send(client_fd, reply.c_str(), reply.size(), 0);
  }

  void handleBLPOP(vector<string> &cmd, int client_fd)
  {
    if (cmd.size() < 3)
      return;

    string key = cmd[1];
    int timeoutSeconds = stoi(cmd[2]) > 0 ? stoi(cmd[2]) : 1;

    unique_lock<mutex> lock(mtx);

    bool found = cv.wait_for(lock, chrono::seconds(timeoutSeconds), [&]()
                             {
          auto it = List.find(key);
          return it != List.end() && !it->second.empty(); });

    if (found)
    {
      string result = List[key][0];
      List[key].erase(List[key].begin());

      string reply = "*2\r\n";
      reply += "$" + to_string(key.size()) + "\r\n" + key + "\r\n";
      reply += "$" + to_string(result.size()) + "\r\n" + result + "\r\n";

      send(client_fd, reply.c_str(), reply.size(), 0);
    }
    else
    {
      const char *timeoutReply = "*-1\r\n";
      send(client_fd, timeoutReply, strlen(timeoutReply), 0);
    }
  }

  void handleTYPE(vector<string> &cmd, int client_fd)
  {

    if (cmd.size() < 2)
      return;

    string key = cmd[1];

    auto it = Database.find(key);
    string result;

    if (Database.find(key) != Database.end())
      result = "string";
    else if (List.find(key) != List.end() && !List[key].empty())
      result = "list";
    else if (Streams.find(key) != Streams.end() && !Streams[key].empty())
      result = "stream";
    else
      result = "none";

    string reply = "+" + result + "\r\n";
    send(client_fd, reply.c_str(), reply.size(), 0);
  }

  void handleXADD(vector<string> &cmd, int client_fd)
  {
    if (cmd.size() < 5)
      return;

    string streamKey = cmd[1];
    string ID = cmd[2];
    map<string, string> TempMap;
    if (ID == "0-0") {
        string reply = "-ERR The ID specified in XADD must be greater than 0-0\r\n";
        send(client_fd, reply.c_str(), reply.size(), 0);
        return;
    }
    for (int i = 3; i < cmd.size() - 1; i += 2)
    {

      TempMap[cmd[i]] = cmd[i + 1];
    }

    auto acceptEntry = [&]() {
      Streams[streamKey].push_back({ID, TempMap});
      string reply = "$" + to_string(ID.size()) + "\r\n" + ID + "\r\n";
      send(client_fd, reply.c_str(), reply.size(), 0);
    };

    auto rejectEntry = [&](){
      string reply = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
      send(client_fd, reply.c_str(), reply.size(), 0);
    };

    if (!Streams[streamKey].empty())
    {
      string lastID = Streams[streamKey].back().first;
      stringstream ss(ID);
      string ms, seq;
      getline(ss, ms, '-');
      getline(ss, seq, '-');

      stringstream ssl(lastID);
      string ms2, seq2;
      getline(ssl, ms2, '-');
      getline(ssl, seq2, '-');

      if(stol(ms) > stol(ms2)){
        acceptEntry();
      }else if(stol(ms) == stol(ms2)){
        if(stol(seq) > stol(seq2)){
          acceptEntry();
        }else{
          rejectEntry();
        }
      }else{
          rejectEntry();
      }

    }else {
    acceptEntry();
}

    
  }
};

ListStorage storage;

void handleCommand(vector<string> &cmd, int client_fd)
{

  if (cmd.empty())
    return;

  if (cmd[0] == "PING" || cmd[0] == "ping")
    storage.handlePing(cmd, client_fd);
  else if (cmd[0] == "ECHO" || cmd[0] == "echo")
    storage.handleEcho(cmd, client_fd);
  else if (cmd[0] == "SET" || cmd[0] == "set")
    storage.handleSET(cmd, client_fd);
  else if (cmd[0] == "GET" || cmd[0] == "get")
    storage.handleGET(cmd, client_fd);
  else if (cmd[0] == "RPUSH" || cmd[0] == "rpush")
    storage.handleRPUSH(cmd, client_fd);
  else if (cmd[0] == "LPUSH" || cmd[0] == "LPUSH")
    storage.handleLPUSH(cmd, client_fd);
  else if (cmd[0] == "LRANGE" || cmd[0] == "lrange")
    storage.handleLRANGE(cmd, client_fd);
  else if (cmd[0] == "LLEN" || cmd[0] == "llen")
    storage.handleLLEN(cmd, client_fd);
  else if (cmd[0] == "LPOP" || cmd[0] == "lpop")
    storage.handleLPOP(cmd, client_fd);
  else if (cmd[0] == "BLPOP" || cmd[0] == "blpop")
    storage.handleBLPOP(cmd, client_fd);
  else if (cmd[0] == "TYPE" || cmd[0] == "type")
    storage.handleTYPE(cmd, client_fd);
  else if (cmd[0] == "XADD" || cmd[0] == "xadd")
    storage.handleXADD(cmd, client_fd);
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

    handleCommand(cmd, client_fd);
  }
  close(client_fd);
}

int main()
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
