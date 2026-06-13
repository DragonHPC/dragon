#include <stdio.h>
#include "err.h"
#include "dragon/messages_api.h"

int main(int argc, char *argv[]) {

  dragonError_t err;
  err = dragon_logging_attach();
  if (err != DRAGON_SUCCESS) {
    printf("Error attaching to logging FLI: %s\n", dragon_getlasterrstr(err));
    return -1;
  }

  err = dragon_log_message(
    "user_app",        /* name */
    "Hello from C!",   /* message */
    NULL,              /* time */
    NULL,              /* func */
    NULL,              /* hostname */
    NULL,              /* ipAddress */
    0,                 /* port */
    NULL,              /* service */
    LOG_LEVEL_INFO,    /* level */
    NULL               /* timeout */
  );
  if (err != DRAGON_SUCCESS) {
    printf("Error logging message: %s\n", dragon_getlasterrstr(err));
    return -1;
  }

  err = dragon_logging_detach();
  if (err != DRAGON_SUCCESS) {
    printf("Error detaching from logging FLI: %s\n", dragon_getlasterrstr(err));
    return -1;
  }

  return 0;
}