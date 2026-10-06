#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#define PORT 6452

int main(int argc, char *argv[])
{
    const char *server_ip = "127.0.0.1";

    if (argc > 2) {
        fprintf(stderr, "Usage: %s [server_ipv4]\n", argv[0]);
        return EXIT_FAILURE;
    }

    if (argc == 2)
        server_ip = argv[1];

    int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd < 0) {
        perror("socket");
        return EXIT_FAILURE;
    }

    struct sockaddr_in address = {0};
    address.sin_family = AF_INET;
    address.sin_port = htons(PORT);

    if (inet_pton(AF_INET, server_ip, &address.sin_addr) != 1) {
        fprintf(stderr, "Invalid IPv4 address\n");
        close(socket_fd);
        return EXIT_FAILURE;
    }

    if (connect(socket_fd, (struct sockaddr *)&address,
                sizeof(address)) < 0) {
        perror("connect");
        close(socket_fd);
        return EXIT_FAILURE;
    }

    printf("Connected to %s:%d\n", server_ip, PORT);
    fflush(stdout);

    char buffer[1024];

    while (1) {
        ssize_t n = recv(socket_fd, buffer, sizeof(buffer), 0);

        if (n < 0) {
            if (errno == EINTR)
                continue;
            perror("recv");
            close(socket_fd);
            return EXIT_FAILURE;
        }

        if (n == 0)
            break;

        if (fwrite(buffer, 1, (size_t)n, stdout) != (size_t)n) {
            fprintf(stderr, "Could not display server message\n");
            close(socket_fd);
            return EXIT_FAILURE;
        }

        fflush(stdout);
    }

    printf("Server closed the connection\n");
    close(socket_fd);
    return EXIT_SUCCESS;
}
