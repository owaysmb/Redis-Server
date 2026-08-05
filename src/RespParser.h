#pragma once
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

string encodeRESPArray(const vector<string> &cmd)
{

  string fullString = "*" + to_string(cmd.size()) + "\r\n";

    for (int i = 0; i < cmd.size(); i++)
    {
        fullString += "$" + to_string(cmd[i].length()) + "\r\n" + cmd[i] + "\r\n";
    }

    return fullString;
}