#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <poll.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#define PORT 6452
#define MAX_LINE 2048

static int send_all(int fd, const char *message, size_t length)
{
    size_t sent = 0;

    while (sent < length) {
        ssize_t n = send(fd, message + sent, length - sent, 0);
        if (n < 0 && errno == EINTR)
            continue;
        if (n <= 0)
            return -1;
        sent += (size_t)n;
    }

    return 0;
}

int main(int argc, char *argv[])
{
    const char *server_ip = "127.0.0.1";

    if (argc > 2) {
        fprintf(stderr, "Usage: %s [server_ipv4]\n", argv[0]);
        return EXIT_FAILURE;
    }

    if (argc == 2)
        server_ip = argv[1];

    signal(SIGPIPE, SIG_IGN);

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return EXIT_FAILURE;
    }

    struct sockaddr_in address = {0};
    address.sin_family = AF_INET;
    address.sin_port = htons(PORT);

    if (inet_pton(AF_INET, server_ip, &address.sin_addr) != 1) {
        fprintf(stderr, "Invalid IPv4 address\n");
        close(fd);
        return EXIT_FAILURE;
    }

    if (connect(fd, (struct sockaddr *)&address,
                sizeof(address)) < 0) {
        perror("connect");
        close(fd);
        return EXIT_FAILURE;
    }

    printf("Connected to %s:%d\n", server_ip, PORT);
    printf("First type: REGISTER <username>\n");
    printf("Then use LIST or QUIT\n");
    fflush(stdout);

    char line[MAX_LINE];
    size_t used = 0;
    int discard = 0;
    int input_open = 1;
    int result = EXIT_SUCCESS;

    while (1) {
        struct pollfd fds[2] = {
            {fd, POLLIN, 0},
            {input_open ? STDIN_FILENO : -1, POLLIN, 0}
        };

        if (poll(fds, 2, -1) < 0) {
            if (errno == EINTR)
                continue;
            perror("poll");
            result = EXIT_FAILURE;
            break;
        }

        if (fds[0].revents & (POLLIN | POLLHUP | POLLERR)) {
            char buffer[4096];
            ssize_t n = recv(fd, buffer, sizeof(buffer), 0);

            if (n < 0) {
                if (errno == EINTR)
                    continue;
                perror("recv");
                result = EXIT_FAILURE;
                break;
            }

            if (n == 0) {
                printf("Server closed the connection\n");
                break;
            }

            if (fwrite(buffer, 1, (size_t)n, stdout) != (size_t)n) {
                fprintf(stderr, "Could not display server response\n");
                result = EXIT_FAILURE;
                break;
            }
            fflush(stdout);
        }

        if (fds[1].revents & (POLLIN | POLLHUP)) {
            char buffer[4096];
            ssize_t n = read(STDIN_FILENO, buffer, sizeof(buffer));

            if (n < 0) {
                if (errno == EINTR)
                    continue;
                perror("read");
                result = EXIT_FAILURE;
                break;
            }

            if (n == 0) {
                input_open = 0;
                if (send_all(fd, "QUIT\n", 5) < 0) {
                    perror("send");
                    result = EXIT_FAILURE;
                    break;
                }
                continue;
            }

            for (ssize_t i = 0; i < n; i++) {
                char ch = buffer[i];

                if (ch == '\n') {
                    if (!discard && used > 0) {
                        if (line[used - 1] == '\r')
                            used--;

                        line[used++] = '\n';
                        int quitting =
                            used == 5 && memcmp(line, "QUIT\n", 5) == 0;

                        if (send_all(fd, line, used) < 0) {
                            perror("send");
                            close(fd);
                            return EXIT_FAILURE;
                        }

                        if (quitting)
                            input_open = 0;
                    }

                    used = 0;
                    discard = 0;

                    if (!input_open)
                        break;
                } else if (!discard) {
                    if (ch == '\0' || used >= sizeof(line) - 2) {
                        fprintf(stderr,
                                "Invalid or too long command; line discarded\n");
                        used = 0;
                        discard = 1;
                    } else {
                        line[used++] = ch;
                    }
                }
            }
        }
    }

    close(fd);
    return result;
}
