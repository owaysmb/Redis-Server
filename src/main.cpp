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

using namespace std;
bool Multi = false;

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
bool isNumber(const string &str)
{
  if (str.empty())
    return false;
  return str.find_first_not_of("0123456789") == std::string::npos;
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
  vector<vector<string>> Q;
  int ExecCounts = 0;

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
    auto now = chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    auto millisec = chrono::duration_cast<chrono::milliseconds>(duration).count();

    if (ID == "*")
    {
      ID = to_string(millisec) + "-" + "0";
    }

    if (ID == "0-0")
    {
      string reply = "-ERR The ID specified in XADD must be greater than 0-0\r\n";
      send(client_fd, reply.c_str(), reply.size(), 0);
      return;
    }
    for (int i = 3; i < cmd.size() - 1; i += 2)
    {

      TempMap[cmd[i]] = cmd[i + 1];
    }

    auto IsEmpty = [&]()
    {
      {
        lock_guard<mutex> lock(mtx);
        Streams[streamKey].push_back({ID, TempMap});
      }
      cv.notify_all();
      string reply = "$" + to_string(ID.size()) + "\r\n" + ID + "\r\n";
      send(client_fd, reply.c_str(), reply.size(), 0);
    };

    auto rejectEntry = [&]()
    {
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

      if (seq == "*")
      {
        if (stoi(ms) == stoi(ms2))
          seq = to_string(stoi(seq2) + 1);
        else
          seq = "0";

        ID = ms + "-" + seq;
      }
      auto acceptEntry = [&]()
      {
        {
          lock_guard<mutex> lock(mtx);
          Streams[streamKey].push_back({ID, TempMap});
        }
        cv.notify_all();
        string reply = "$" + to_string(ID.size()) + "\r\n" + ID + "\r\n";
        send(client_fd, reply.c_str(), reply.size(), 0);
      };

      if (stol(ms) > stol(ms2))
      {
        acceptEntry();
      }
      else if (stol(ms) == stol(ms2))
      {
        if (stol(seq) > stol(seq2))
        {
          acceptEntry();
        }
        else
        {
          rejectEntry();
        }
      }
      else
      {
        rejectEntry();
      }
    }
    else
    {
      stringstream ss(ID);
      string ms, seq;
      getline(ss, ms, '-');
      getline(ss, seq, '-');

      if (seq == "*")
      {
        if (ms == "0")
          seq = "1";
        else
          seq = "0";

        ID = ms + "-" + seq;
      }
      IsEmpty();
    }
  }

  void handleXRANGE(vector<string> &cmd, int client_fd)
  {
    if (cmd.size() < 4)
      return;

    string streamKey = cmd[1];
    string start = cmd[2];
    string end = cmd[3];

    auto it = Streams.find(streamKey);

    if (it == Streams.end())
    {
      send(client_fd, "*0\r\n", 4, 0);
      return;
    }

    if (start == "-")
    {
      start = Streams[streamKey].front().first;
    }
    if (end == "+")
    {
      end = Streams[streamKey].back().first;
    }

    stringstream ss(start);
    string ms, seq;
    getline(ss, ms, '-');
    getline(ss, seq, '-');

    stringstream sse(end);
    string mse, seqe;
    getline(sse, mse, '-');
    getline(sse, seqe, '-');

    vector<pair<string, map<string, string>>> matches;

    for (const auto &[entryID, fields] : it->second)
    {

      stringstream ssc(entryID);
      string msc, seqc;
      getline(ssc, msc, '-');
      getline(ssc, seqc, '-');

      if (stol(seqc) >= stol(seq) && stol(seqc) <= stol(seqe))
      {
        matches.push_back({entryID, fields});
      }
    }

    string reply = "*" + to_string(matches.size()) + "\r\n";
    for (const auto &[entryID, fields] : matches)
    {
      reply += "*2\r\n";
      reply += "$" + to_string(entryID.size()) + "\r\n" + entryID + "\r\n";
      reply += "*" + to_string(fields.size() * 2) + "\r\n";
      for (const auto &[k, v] : fields)
      {
        reply += "$" + to_string(k.size()) + "\r\n" + k + "\r\n";
        reply += "$" + to_string(v.size()) + "\r\n" + v + "\r\n";
      }
    }

    send(client_fd, reply.c_str(), reply.size(), 0);
  }

  void handleXREAD(vector<string> &cmd, int client_fd)
  {

    if (cmd.size() < 4)
      return;

    int TotalKeysCount = ceil((cmd.size() - 2) / 2);
    vector<string> TotalStreamKeys;
    vector<string> TotalIDs;

    for (int i = 2; i < (cmd.size() - TotalKeysCount); i++)
      TotalStreamKeys.push_back(cmd[i]);

    for (int i = (2 + TotalKeysCount); i < cmd.size(); i++)
      TotalIDs.push_back(cmd[i]);

    string reply = "*" + to_string(TotalKeysCount) + "\r\n";

    for (int i = 0; i < TotalStreamKeys.size(); i++)
    {
      auto it = Streams.find(TotalStreamKeys[i]);
      if (it == Streams.end())
        continue;

      reply += "*2\r\n";
      reply += "$" + to_string(TotalStreamKeys[i].size()) + "\r\n" + TotalStreamKeys[i] + "\r\n";
      reply += "*1\r\n";

      for (auto [k, v] : it->second)
      {
        reply += "*2\r\n";
        reply += "$" + to_string(k.size()) + "\r\n" + k + "\r\n";
        reply += "*" + to_string(v.size() * 2) + "\r\n";
        for (auto [t, h] : v)
        {
          reply += "$" + to_string(t.size()) + "\r\n" + t + "\r\n";
          reply += "$" + to_string(h.size()) + "\r\n" + h + "\r\n";
        }
      }
    }

    send(client_fd, reply.c_str(), reply.size(), 0);
  }

  void handleXREAD_BLOCK(vector<string> &cmd, int client_fd)
  {
    if (cmd.size() < 6)
      return;

    int Time = stoi(cmd[2]);
    string Key = cmd[4];
    string ID = cmd[5];

    unique_lock<mutex> lock(mtx);
    string thresholdID;
    if (ID == "$")
    {
      auto it = Streams.find(Key);
      if (it != Streams.end() && !it->second.empty())
        thresholdID = it->second.back().first;
      else
        thresholdID = "0-0";
    }
    else
    {
      thresholdID = ID;
    }

    auto predicate = [&]()
    {
      auto it = Streams.find(Key);
      if (it == Streams.end() || it->second.empty())
        return false;
      string lastID = it->second.back().first;
      string ms1, seq1, ms2, seq2;
      stringstream ss1(lastID), ss2(thresholdID);
      getline(ss1, ms1, '-');
      getline(ss1, seq1, '-');
      getline(ss2, ms2, '-');
      getline(ss2, seq2, '-');
      return stol(ms1) > stol(ms2) ||
             (stol(ms1) == stol(ms2) && stol(seq1) > stol(seq2));
    };

    auto buildReply = [&]()
    {
      string reply = "*1\r\n";
      reply += "*2\r\n";
      reply += "$" + to_string(Key.size()) + "\r\n" + Key + "\r\n";

      auto it = Streams.find(Key);
      if (it == Streams.end())
        return reply;

      string entriesReply = "";
      int count = 0;
      for (auto [k, v] : it->second)
      {
        string ms1, seq1, ms2, seq2;
        stringstream ss1(k), ss2(thresholdID);
        getline(ss1, ms1, '-');
        getline(ss1, seq1, '-');
        getline(ss2, ms2, '-');
        getline(ss2, seq2, '-');
        bool greater = stol(ms1) > stol(ms2) ||
                       (stol(ms1) == stol(ms2) && stol(seq1) > stol(seq2));
        if (!greater)
          continue;

        count++;
        entriesReply += "*2\r\n";
        entriesReply += "$" + to_string(k.size()) + "\r\n" + k + "\r\n";
        entriesReply += "*" + to_string(v.size() * 2) + "\r\n";
        for (auto [t, h] : v)
        {
          entriesReply += "$" + to_string(t.size()) + "\r\n" + t + "\r\n";
          entriesReply += "$" + to_string(h.size()) + "\r\n" + h + "\r\n";
        }
      }
      reply += "*" + to_string(count) + "\r\n";
      reply += entriesReply;
      return reply;
    };

    if (Time == 0)
    {
      cv.wait(lock, predicate);
      string reply = buildReply();
      send(client_fd, reply.c_str(), reply.size(), 0);
    }
    else
    {
      bool found = cv.wait_for(lock, chrono::milliseconds(Time), predicate);
      if (found)
      {
        string reply = buildReply();
        send(client_fd, reply.c_str(), reply.size(), 0);
      }
      else
      {
        const char *timeoutReply = "*-1\r\n";
        send(client_fd, timeoutReply, strlen(timeoutReply), 0);
      }
    }
  }

  void handleINCR(vector<string> &cmd, int client_fd)
  {
    string Key = cmd[1];
    auto it = Database.find(Key);

    if (it == Database.end())
    {
      Database[Key] = "1";
      string reply = ":1\r\n";
      send(client_fd, reply.c_str(), reply.size(), 0);
    }
    else
    {
      if (isNumber(Database[Key]))
      {
        int v = stoi(it->second);
        v++;
        it->second = to_string(v);
        string reply = ":" + it->second + "\r\n";
        send(client_fd, reply.c_str(), reply.size(), 0);
      }
      else
      {
        string reply = "-ERR value is not an integer or out of range\r\n";
        send(client_fd, reply.c_str(), reply.size(), 0);
      }
    }
  }

  void handleQueuing(vector<string> &cmd, int client_fd){
    Q.push_back(cmd);
    const char *reply = "+QUEUED\r\n";
    send(client_fd, reply, strlen(reply), 0);
  }

  void handleMULTI(vector<string> &cmd, int client_fd)
  {
    Multi = true;
    const char *reply = "+OK\r\n";
    send(client_fd, reply, strlen(reply), 0);
  }

  void handleEXEC(vector<string> &cmd, int client_fd)
  {
    ExecCounts++;
    if (!Multi)
    {
      string reply = "-ERR EXEC without MULTI\r\n";
      send(client_fd, reply.c_str(), reply.size(), 0);
    }
    else
    {
      if (Q.empty() && ExecCounts == 1)
      {
        string reply = "*0\r\n";
        send(client_fd, reply.c_str(), reply.size(), 0);
      }else{
        string reply = "-ERR EXEC without MULTI\r\n";
        send(client_fd, reply.c_str(), reply.size(), 0);
      }
    }
    
    for (auto &&v : Q)
    {
     
        if (v[0] == "GET") {
          const char *nullReply = "$-1\r\n";
          send(client_fd, nullReply, strlen(nullReply), 0);
        }
      
      
      
    }
    

    Q.clear();
    Multi = false;

  }

  

};

ListStorage storage;

void handleCommand(vector<string> &cmd, int client_fd)
{

  if (cmd.empty())
    return;

  if(Multi && cmd[0] != "EXEC" && cmd[0] != "MULTI"){
    storage.handleQueuing(cmd,client_fd);
    return;  
  }
    

  
  if (cmd[0] == "MULTI")
    storage.handleMULTI(cmd, client_fd);
  else if (cmd[0] == "EXEC")
    storage.handleEXEC(cmd, client_fd); 
  else if (cmd[0] == "PING" || cmd[0] == "ping")
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
  else if (cmd[0] == "XRANGE" || cmd[0] == "xrange")
    storage.handleXRANGE(cmd, client_fd);
  else if (cmd[0] == "XREAD" && cmd[1] == "block" 
    || cmd[0] == "XREAD" && cmd[1] == "BLOCK")
    storage.handleXREAD_BLOCK(cmd, client_fd);
  else if (cmd[0] == "XREAD")
    storage.handleXREAD(cmd, client_fd);
  else if (cmd[0] == "INCR")
    storage.handleINCR(cmd, client_fd);
  
  
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
