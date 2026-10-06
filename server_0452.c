#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <poll.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/time.h>

#define PORT 6452
#define MAX_CLIENTS 32
#define MAX_NAME 31
#define MAX_LINE 2048
#define TAG " NID:6204\n"

typedef struct {
    int fd;
    char username[MAX_NAME + 1];
    char input[MAX_LINE];
    size_t used;
} Client;

static Client clients[MAX_CLIENTS];

static int send_all(int fd, const char *message)
{
    size_t sent = 0;
    size_t length = strlen(message);

    while (sent < length) {
        ssize_t n = send(fd, message + sent, length - sent, 0);
        if (n < 0 && errno == EINTR)
            continue;
        if (n <= 0) {
            shutdown(fd, SHUT_RDWR);
            return -1;
        }
        sent += (size_t)n;
    }
    return 0;
}

static void notify_others(int excluded, const char *message)
{
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (i != excluded && clients[i].fd >= 0 &&
            clients[i].username[0] != '\0')
            send_all(clients[i].fd, message);
    }
}

static void remove_client(int index)
{
    Client *c = &clients[index];
    if (c->fd < 0)
        return;

    char username[MAX_NAME + 1];
    strcpy(username, c->username);
    close(c->fd);
    c->fd = -1;
    c->username[0] = '\0';
    c->used = 0;

    if (username[0] != '\0') {
        char message[128];
        snprintf(message, sizeof(message),
                 "MSG LEAVE %s\n", username);
        notify_others(index, message);
        printf("User left: %s\n", username);
    } else {
        printf("Unregistered client disconnected\n");
    }
    fflush(stdout);
}

static int valid_name(const char *name)
{
    size_t length = strlen(name);
    if (length == 0 || length > MAX_NAME)
        return 0;

    for (size_t i = 0; i < length; i++) {
        unsigned char ch = (unsigned char)name[i];
        if (!((ch >= 'a' && ch <= 'z') ||
              (ch >= 'A' && ch <= 'Z') ||
              (ch >= '0' && ch <= '9') ||
              ch == '_' || ch == '-'))
            return 0;
    }
    return 1;
}

static void handle_command(int index, char *line)
{
    Client *c = &clients[index];
    char reply[MAX_LINE + MAX_NAME + 64];

    if (strcmp(line, "QUIT") == 0) {
        send_all(c->fd, "OK BYE" TAG);
        remove_client(index);
        return;
    }

    if (strncmp(line, "REGISTER ", 9) == 0) {
        const char *name = line + 9;

        if (c->username[0] != '\0') {
            send_all(c->fd, "ERR 005 ALREADY_REGISTERED" TAG);
            return;
        }

        if (!valid_name(name)) {
            send_all(c->fd, "ERR 005 INVALID_USERNAME" TAG);
            return;
        }

        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].fd >= 0 &&
                strcmp(clients[i].username, name) == 0) {
                send_all(c->fd, "ERR 001 USERNAME_TAKEN" TAG);
                return;
            }
        }

        strcpy(c->username, name);
        snprintf(reply, sizeof(reply),
                 "OK REGISTERED %s" TAG, name);

        if (send_all(c->fd, reply) < 0) {
            remove_client(index);
            return;
        }

        snprintf(reply, sizeof(reply), "MSG JOIN %s\n", name);
        notify_others(index, reply);
        printf("User registered: %s\n", name);
        fflush(stdout);
        return;
    }

    if (c->username[0] == '\0') {
        send_all(c->fd, "ERR 005 REGISTER_FIRST" TAG);
        return;
    }

    if (strcmp(line, "LIST") == 0) {
        strcpy(reply, "OK USERS ");
        int first = 1;

        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].fd >= 0 &&
                clients[i].username[0] != '\0') {
                if (!first)
                    strcat(reply, ",");
                strcat(reply, clients[i].username);
                first = 0;
            }
        }

        strcat(reply, TAG);
        send_all(c->fd, reply);
        return;
    }


    if (strncmp(line, "BCAST ", 6) == 0) {
        const char *message = line + 6;

        if (message[strspn(message, " \t")] == '\0') {
            send_all(c->fd, "ERR 005 EMPTY_MESSAGE" TAG);
            return;
        }

        snprintf(reply, sizeof(reply),
                 "MSG BCAST %s %s\n", c->username, message);
        notify_others(index, reply);
        send_all(c->fd, "OK SENT" TAG);
        return;
    }

    if (strncmp(line, "PMSG ", 5) == 0) {
        char *target = line + 5;
        char *separator = strchr(target, ' ');

        if (separator == NULL) {
            send_all(c->fd, "ERR 005 INVALID_FORMAT" TAG);
            return;
        }

        *separator = '\0';
        const char *message = separator + 1;

        if (!valid_name(target)) {
            send_all(c->fd, "ERR 005 INVALID_USERNAME" TAG);
            return;
        }

        if (message[strspn(message, " \t")] == '\0') {
            send_all(c->fd, "ERR 005 EMPTY_MESSAGE" TAG);
            return;
        }

        int recipient = -1;

        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].fd >= 0 &&
                clients[i].username[0] != '\0' &&
                strcmp(clients[i].username, target) == 0) {
                recipient = i;
                break;
            }
        }

        if (recipient < 0) {
            send_all(c->fd, "ERR 002 USER_NOT_FOUND" TAG);
            return;
        }

        snprintf(reply, sizeof(reply),
                 "MSG PRIV %s %s\n", c->username, message);

        if (send_all(clients[recipient].fd, reply) < 0)
            send_all(c->fd, "ERR 007 DELIVERY_FAILED" TAG);
        else
            send_all(c->fd, "OK SENT" TAG);

        return;
    }

    send_all(c->fd, "ERR 005 INVALID_COMMAND" TAG);
}

static void read_client(int index)
{
    Client *c = &clients[index];
    char buffer[4096];

    ssize_t n = recv(c->fd, buffer, sizeof(buffer), MSG_DONTWAIT);

    if (n < 0) {
        if (errno == EINTR || errno == EAGAIN ||
            errno == EWOULDBLOCK)
            return;
        remove_client(index);
        return;
    }

    if (n == 0) {
        remove_client(index);
        return;
    }

    for (ssize_t i = 0; i < n; i++) {
        unsigned char ch = (unsigned char)buffer[i];

        if (ch == '\0') {
            send_all(c->fd, "ERR 005 INVALID_TEXT" TAG);
            remove_client(index);
            return;
        }

        if (ch == '\n') {
            c->input[c->used] = '\0';
            if (c->used > 0 && c->input[c->used - 1] == '\r')
                c->input[c->used - 1] = '\0';

            handle_command(index, c->input);
            c->used = 0;

            if (c->fd < 0)
                return;
        } else {
            if (c->used >= sizeof(c->input) - 1) {
                send_all(c->fd, "ERR 005 LINE_TOO_LONG" TAG);
                remove_client(index);
                return;
            }
            c->input[c->used++] = (char)ch;
        }
    }
}

int main(void)
{
    signal(SIGPIPE, SIG_IGN);

    for (int i = 0; i < MAX_CLIENTS; i++)
        clients[i].fd = -1;

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket");
        return EXIT_FAILURE;
    }

    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR,
                   &reuse, sizeof(reuse)) < 0) {
        perror("setsockopt");
        close(server_fd);
        return EXIT_FAILURE;
    }

    struct sockaddr_in address = {0};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr *)&address,
             sizeof(address)) < 0 ||
        listen(server_fd, MAX_CLIENTS) < 0) {
        perror("bind/listen");
        close(server_fd);
        return EXIT_FAILURE;
    }

    printf("NetMessenger server - IT23620452\n");
    printf("Listening on port %d | NID:6204\n", PORT);
    printf("Commands: REGISTER, LIST, BCAST, PMSG, QUIT\n");
    fflush(stdout);

    while (1) {
        struct pollfd fds[MAX_CLIENTS + 1];
        fds[0] = (struct pollfd){server_fd, POLLIN, 0};

        for (int i = 0; i < MAX_CLIENTS; i++)
            fds[i + 1] = (struct pollfd){
                clients[i].fd, POLLIN, 0
            };

        if (poll(fds, MAX_CLIENTS + 1, -1) < 0) {
            if (errno == EINTR)
                continue;
            perror("poll");
            break;
        }

        for (int i = 0; i < MAX_CLIENTS; i++) {
            short events = fds[i + 1].revents;
            if (clients[i].fd < 0)
                continue;

            if (events & POLLIN)
                read_client(i);
            else if (events & (POLLHUP | POLLERR | POLLNVAL))
                remove_client(i);
        }

        if (fds[0].revents & POLLIN) {
            int fd = accept(server_fd, NULL, NULL);
            if (fd < 0) {
                if (errno != EINTR)
                    perror("accept");
                continue;
            }

            struct timeval timeout = {2, 0};
            if (setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO,
                           &timeout, sizeof(timeout)) < 0) {
                perror("send timeout");
                close(fd);
                continue;
            }

            int slot = -1;
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i].fd < 0) {
                    slot = i;
                    break;
                }
            }

            if (slot < 0) {
                send_all(fd, "ERR 006 SERVER_FULL" TAG);
                close(fd);
            } else {
                clients[slot].fd = fd;
                clients[slot].used = 0;
                clients[slot].username[0] = '\0';
                printf("Client connected; waiting for REGISTER\n");
                fflush(stdout);
            }
        }
    }

    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].fd >= 0)
            close(clients[i].fd);
    }
    close(server_fd);
    return EXIT_FAILURE;
}
