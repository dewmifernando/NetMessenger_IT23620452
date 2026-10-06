#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdint.h>
#include <unistd.h>
#include <signal.h>
#include <poll.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/stat.h>

#define PORT 6452
#define MAX_LINE 2048
#define MAX_FILE (1024U * 1024U)
#define MAX_OUTPUT (4U * 1024U * 1024U)

typedef struct {
    char line[MAX_LINE + 128];
    size_t used;
    size_t remaining;
    int receiving;
    FILE *file;
    int failed;
    char temporary[512];
    char destination[512];
    char username[32];
} Receiver;

static unsigned char *outgoing;
static size_t outgoing_used;

static int valid_token(const char *s, size_t limit, int dots)
{
    size_t n = strlen(s);
    if (!n || n > limit || s[0] == '.') return 0;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)s[i];
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || c == '_' || c == '-' || (dots && c == '.')))
            return 0;
    }
    return 1;
}

static int ensure_directory(const char *path)
{
    if (mkdir(path, 0700) == 0) return 0;
    struct stat st;
    return errno == EEXIST && lstat(path, &st) == 0 && S_ISDIR(st.st_mode) ? 0 : -1;
}

static int enqueue(const void *data, size_t size)
{
    if (size > MAX_OUTPUT - outgoing_used) {
        fprintf(stderr, "Outgoing queue full; wait before sending more\n");
        return -1;
    }
    unsigned char *p = realloc(outgoing, outgoing_used + size);
    if (!p && size) { perror("realloc"); return -1; }
    outgoing = p;
    if (size) memcpy(outgoing + outgoing_used, data, size);
    outgoing_used += size;
    return 0;
}

static int command(char *line)
{
    if (strncmp(line, "SENDFILE ", 9) != 0) {
        size_t n = strlen(line);
        line[n++] = '\n';
        return enqueue(line, n);
    }
    char *save;
    char *target = strtok_r(line + 9, " \t", &save);
    char *path = strtok_r(NULL, " \t", &save);
    char *declared = strtok_r(NULL, " \t", &save);
    char *extra = strtok_r(NULL, " \t", &save);
    if (!target || !path || extra || !valid_token(target, 31, 0)) {
        fprintf(stderr, "Use: SENDFILE <user_or_room> <local_file_path> [filesize]\n");
        return -1;
    }
    const char *name = strrchr(path, '/');
    name = name ? name + 1 : path;
    if (!valid_token(name, 127, 1)) {
        fprintf(stderr, "Filename: use letters, digits, _, -, .; no initial dot\n");
        return -1;
    }
    int local_fd = open(path, O_RDONLY);
    if (local_fd < 0) { perror("open file"); return -1; }
    struct stat st;
    if (fstat(local_fd, &st) < 0 || !S_ISREG(st.st_mode) || st.st_size < 0 ||
        (uint64_t)st.st_size > MAX_FILE) {
        fprintf(stderr, "File must be a regular file, at most %u bytes\n", MAX_FILE);
        close(local_fd);
        return -1;
    }
    size_t size = (size_t)st.st_size;
    if (declared) {
        errno = 0;
        char *end;
        unsigned long long given = strtoull(declared, &end, 10);
        if (!*declared || declared[strspn(declared, "0123456789")] || errno ||
            *end || given != size) {
            fprintf(stderr, "Declared filesize must match the local file (%zu bytes)\n", size);
            close(local_fd);
            return -1;
        }
    }
    char header[256];
    int h = snprintf(header, sizeof(header), "SENDFILE %s %s %zu\n", target, name, size);
    size_t total = (size_t)h + size;
    unsigned char *frame = malloc(total);
    if (!frame) { perror("malloc"); close(local_fd); return -1; }
    memcpy(frame, header, (size_t)h);
    size_t used = 0;
    while (used < size) {
        ssize_t n = read(local_fd, frame + h + used, size - used);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) {
            fprintf(stderr, "Could not read complete file; nothing sent\n");
            free(frame); close(local_fd); return -1;
        }
        used += (size_t)n;
    }
    close(local_fd);
    int result = enqueue(frame, total);
    free(frame);
    if (result == 0) printf("Queued file: %s -> %s (%zu bytes)\n", name, target, size);
    return result;
}

static void finish_receive(Receiver *r)
{
    if (r->file) {
        if (fclose(r->file) != 0) r->failed = 1;
        r->file = NULL;
    }
    if (!r->failed && rename(r->temporary, r->destination) != 0) r->failed = 1;
    if (r->failed) {
        if (r->temporary[0]) unlink(r->temporary);
        fprintf(stderr, "Could not save incoming file\n");
    } else printf("File saved: %s\n", r->destination);
    r->temporary[0] = '\0';
    r->receiving = 0;
    fflush(stdout);
}

static int response_line(Receiver *r)
{
    if (strncmp(r->line, "MSG FILE ", 9) == 0) {
        char sender[32], name[128], size_text[32], extra;
        if (sscanf(r->line + 9, "%31s %127s %31s %c", sender, name, size_text, &extra) != 3 ||
            !valid_token(sender, 31, 0) || !valid_token(name, 127, 1) ||
            !*size_text || size_text[strspn(size_text, "0123456789")]) return -1;
        errno = 0;
        char *end;
        unsigned long long size = strtoull(size_text, &end, 10);
        if (errno || *end || size > MAX_FILE || !r->username[0]) return -1;
        printf("Incoming file from %s: %s (%llu bytes)\n", sender, name, size);
        r->remaining = (size_t)size;
        r->receiving = 1;
        r->failed = 0;
        r->temporary[0] = '\0';
        char userdir[128], senderdir[256];
        snprintf(userdir, sizeof(userdir), "received/%s", r->username);
        snprintf(senderdir, sizeof(senderdir), "%s/%s", userdir, sender);
        snprintf(r->destination, sizeof(r->destination), "%s/%s", senderdir, name);
        if (ensure_directory("received") < 0 || ensure_directory(userdir) < 0 ||
            ensure_directory(senderdir) < 0) r->failed = 1;
        else {
            snprintf(r->temporary, sizeof(r->temporary), "%s/.incoming-XXXXXX", senderdir);
            int temp_fd = mkstemp(r->temporary);
            if (temp_fd < 0) r->failed = 1;
            else {
                r->file = fdopen(temp_fd, "wb");
                if (!r->file) { close(temp_fd); r->failed = 1; }
            }
        }
        if (!size) finish_receive(r);
    } else {
        if (strncmp(r->line, "OK REGISTERED ", 14) == 0) {
            char name[32], tag[32], extra;
            if (sscanf(r->line + 14, "%31s %31s %c", name, tag, &extra) == 2 &&
                valid_token(name, 31, 0) && strcmp(tag, "NID:6204") == 0)
                strcpy(r->username, name);
        }
        puts(r->line);
    }
    fflush(stdout);
    return 0;
}

static int receive_bytes(Receiver *r, const unsigned char *data, size_t n)
{
    size_t i = 0;
    while (i < n) {
        if (r->receiving) {
            size_t take = n - i;
            if (take > r->remaining) take = r->remaining;
            if (r->file && !r->failed && fwrite(data + i, 1, take, r->file) != take)
                r->failed = 1;
            i += take;
            r->remaining -= take;
            if (!r->remaining) finish_receive(r);
        } else {
            unsigned char ch = data[i++];
            if (ch == '\n') {
                r->line[r->used] = '\0';
                if (response_line(r) < 0) return -1;
                r->used = 0;
            } else if (!ch || r->used >= sizeof(r->line) - 1) return -1;
            else r->line[r->used++] = (char)ch;
        }
    }
    return 0;
}

int main(int argc, char *argv[])
{
    const char *server_ip = argc == 2 ? argv[1] : "127.0.0.1";
    if (argc > 2) {
        fprintf(stderr, "Usage: %s [server_ipv4]\n", argv[0]); return EXIT_FAILURE;
    }
    signal(SIGPIPE, SIG_IGN);
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return EXIT_FAILURE; }
    struct sockaddr_in address = {0};
    address.sin_family = AF_INET;
    address.sin_port = htons(PORT);
    if (inet_pton(AF_INET, server_ip, &address.sin_addr) != 1) {
        fprintf(stderr, "Invalid IPv4 address\n"); close(fd); return EXIT_FAILURE;
    }
    if (connect(fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("connect"); close(fd); return EXIT_FAILURE;
    }
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        perror("nonblocking socket"); close(fd); return EXIT_FAILURE;
    }
    printf("Connected to %s:%d\nFirst type: REGISTER <username>\n", server_ip, PORT);
    printf("Commands: LIST, BCAST, PMSG, JOIN, LEAVE, ROOMS, RMSG, SENDFILE, QUIT\n");
    printf("File command: SENDFILE <user_or_room> <local_file_path> [filesize]\n");
    fflush(stdout);
    Receiver receiver = {0};
    char line[MAX_LINE];
    size_t used = 0;
    int discard = 0, input_open = 1, result = EXIT_SUCCESS;
    for (;;) {
        struct pollfd fds[2] = {
            {fd, (short)(POLLIN | (outgoing_used ? POLLOUT : 0)), 0},
            {input_open ? STDIN_FILENO : -1, POLLIN, 0}
        };
        if (poll(fds, 2, -1) < 0) {
            if (errno == EINTR) continue;
            perror("poll"); result = EXIT_FAILURE; break;
        }
        if (fds[0].revents & (POLLIN | POLLHUP | POLLERR)) {
            unsigned char buffer[4096];
            ssize_t n = recv(fd, buffer, sizeof(buffer), 0);
            if (n < 0) {
                if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
                    perror("recv"); result = EXIT_FAILURE; break;
                }
            } else if (!n) {
                printf("Server closed the connection\n");
                if (receiver.receiving || receiver.used) {
                    fprintf(stderr, "Incomplete incoming frame\n"); result = EXIT_FAILURE;
                }
                break;
            } else if (receive_bytes(&receiver, buffer, (size_t)n) < 0) {
                fprintf(stderr, "Invalid server frame\n"); result = EXIT_FAILURE; break;
            }
        }
        if ((fds[0].revents & POLLOUT) && outgoing_used) {
            ssize_t n = send(fd, outgoing, outgoing_used, 0);
            if (n < 0) {
                if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
                    perror("send"); result = EXIT_FAILURE; break;
                }
            } else if (!n) { result = EXIT_FAILURE; break; }
            else {
                outgoing_used -= (size_t)n;
                if (outgoing_used) memmove(outgoing, outgoing + n, outgoing_used);
                else { free(outgoing); outgoing = NULL; }
            }
        }
        if (fds[1].revents & (POLLIN | POLLHUP)) {
            char buffer[4096];
            ssize_t n = read(STDIN_FILENO, buffer, sizeof(buffer));
            if (n < 0) {
                if (errno == EINTR) continue;
                perror("read"); result = EXIT_FAILURE; break;
            }
            if (!n) {
                input_open = 0;
                if (enqueue("QUIT\n", 5) < 0) { result = EXIT_FAILURE; break; }
                continue;
            }
            for (ssize_t i = 0; i < n; i++) {
                char ch = buffer[i];
                if (ch == '\n') {
                    if (!discard && used) {
                        if (line[used - 1] == '\r') used--;
                        line[used] = '\0';
                        int quitting = strcmp(line, "QUIT") == 0;
                        if (command(line) == 0 && quitting) input_open = 0;
                    }
                    used = 0; discard = 0;
                    if (!input_open) break;
                } else if (!discard) {
                    if (!ch || used >= sizeof(line) - 2) {
                        fprintf(stderr, "Invalid or too long command; line discarded\n");
                        used = 0; discard = 1;
                    } else line[used++] = ch;
                }
            }
        }
    }
    if (receiver.file) fclose(receiver.file);
    if (receiver.temporary[0]) unlink(receiver.temporary);
    free(outgoing);
    close(fd);
    return result;
}