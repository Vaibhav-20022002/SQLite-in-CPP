#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <unistd.h>

/* Constants for Meta Commands  */
typedef enum {
  META_COMMAND_SUCCESS,
  META_UNRECOGNIZED_COMMAND
} META_COMMAND_RESULT;

/* Constants for Commmands' Status */
typedef enum { PREPARE_SUCCESS, PREPARE_UNRECOGNIZED_STATE } PREPARE_RESULT;

/* Constants for Command type */
typedef enum { COMMAND_SELECT, COMMAND_INSERT } COMMAND;

/**
 * @brief Displays the default SQLite prompt
 */
void displayDefault() { std::cout << "SQLite > "; }

/**
 * @brief Cleanup function called before exit
 */
void closeInput() { std::cout << "Goodbye!\n"; }

/**
 * @brief   Efficiently reads a line of input using std::getline
 * @param   inputBuffer Reference to string where input will be stored
 * @return  Number of bytes read, -1 on error
 */
ssize_t readLine(std::string &inputBuffer) {
  // Clear any existing content for safety
  inputBuffer.clear();

  // Read entire line at once
  if (!std::getline(std::cin, inputBuffer)) {
    // Handle EOF or error
    return -1;
  }

  return inputBuffer.size();
}

META_COMMAND_RESULT selectAndDoMetaCommand(const std::string &inputLine) {
  if (inputLine == ".exit") {
    closeInput();
    exit(EXIT_SUCCESS);
  } else {
    return META_UNRECOGNIZED_COMMAND;
  }
}

/* Matches the command with their type */
PREPARE_RESULT prepareCommand(const std::string &inputLine, int &commandType) {
  std::string commandPrefix = inputLine.substr(0, 6);
  if (commandPrefix == "select") {
    commandType = COMMAND_SELECT;
    return PREPARE_SUCCESS;
  } else if (commandPrefix == "insert") {
    commandType = COMMAND_INSERT;
    return PREPARE_SUCCESS;
  }
  return PREPARE_UNRECOGNIZED_STATE;
}

/* Execute the logic behind the command */
void executeCommand(int &commandType) {
  switch (commandType) {
  case COMMAND_INSERT:
    std::cout << "Executing INSERT Command...\n";
    break;
  case COMMAND_SELECT:
    std::cout << "Executing SELECT Command...\n";
    break;
  }
}

/**
 * @brief   Main loop for taking input, runs infinte loop, gets a line and
 *          processes it
 */
int main(int argc, char **argv) {
  std::string inputLine;

  while (true) {
    displayDefault();

    if ((readLine(inputLine)) < 0) {
      if (std::cin.eof()) {
        std::cout << "\n"; // Add newline for EOF
        closeInput();
        return EXIT_SUCCESS;
      }
      std::cerr << "Error reading input\n";
      return EXIT_FAILURE;
    }

    if (!inputLine.size()) {
      std::cout << "Unrecognized Input :\n";
      continue;
    }

    /* Handling Meta commands */
    if (inputLine[0] == '.') {
      switch (selectAndDoMetaCommand(inputLine)) {
      case (META_COMMAND_SUCCESS):
        continue;
      case (META_UNRECOGNIZED_COMMAND):
        std::cout << "Unexpected Input: '" << inputLine << "'\n";
        continue;
      }
    }

    /* Handling Non-Meta commands */
    int commandType = -1;
    switch (prepareCommand(inputLine, commandType)) {
    case PREPARE_SUCCESS:
      break;
    case PREPARE_UNRECOGNIZED_STATE:
      std::cout << "Unrecognized keyword at the start of '" << inputLine
                << "'\n";
      continue;
    }

    /* execute the command */
    executeCommand(commandType);
    std::cout << "Executed\n";
  }

  std::cout << "Unknown error occurred. Exiting...\n";
  return EXIT_FAILURE;
}
