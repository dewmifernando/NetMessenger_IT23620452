#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdint.h>
#include <inttypes.h>
#include <unistd.h>
#include <signal.h>
#include <poll.h>
#include <fcntl.h>
#include <time.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/stat.h>

#define PORT 6452
#define MAX_CLIENTS 32
#define MAX_NAME 31
#define MAX_LINE 2048
#define MAX_ROOMS 32
#define MAX_FILE (1024U * 1024U)
#define MAX_OUTPUT (4U * 1024U * 1024U)
#define TAG " NID:6204\n"
#define STORAGE "storage/IT23620452"

typedef struct {
    int fd;
    char username[MAX_NAME + 1];
    char input[MAX_LINE];
    size_t used;
    unsigned char *output;
    size_t output_used;
    int closing;
    uint64_t remaining;
    size_t file_size;
    int receiving;
    FILE *upload;
    char filename[128];
    char temporary[512];
    char destination[512];
    int recipients[MAX_CLIENTS];
    unsigned long generations[MAX_CLIENTS];
    unsigned long generation;
    const char *upload_error;
    time_t last_activity;
} Client;

typedef struct {
    char name[MAX_NAME + 1];
    int members[MAX_CLIENTS];
} Room;

static Client clients[MAX_CLIENTS];
static Room rooms[MAX_ROOMS];
static unsigned long next_generation;

static int find_room(const char *name)
{
    for (int i = 0; i < MAX_ROOMS; i++)
        if (rooms[i].name[0] && strcmp(rooms[i].name, name) == 0)
            return i;
    return -1;
}

static int valid_name(const char *name)
{
    size_t n = strlen(name);
    if (!n || n > MAX_NAME) return 0;
    for (size_t i = 0; i < n; i++) {
        unsigned char ch = (unsigned char)name[i];
        if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
              (ch >= '0' && ch <= '9') || ch == '_' || ch == '-'))
            return 0;
    }
    return 1;
}

static int valid_filename(const char *name)
{
    size_t n = strlen(name);
    if (!n || n >= 128 || name[0] == '.') return 0;
    for (size_t i = 0; i < n; i++) {
        unsigned char ch = (unsigned char)name[i];
        if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
              (ch >= '0' && ch <= '9') || ch == '_' || ch == '-' || ch == '.'))
            return 0;
    }
    return 1;
}

/* Enqueue complete frames; only the poll loop writes to client sockets. */
static int queue_bytes(Client *c, const void *data, size_t length)
{
    if (c->fd < 0 || c->closing || length > MAX_OUTPUT - c->output_used)
        return -1;
    unsigned char *p = realloc(c->output, c->output_used + length);
    if (!p && length) {
        shutdown(c->fd, SHUT_RDWR);
        return -1;
    }
    c->output = p;
    if (length) memcpy(c->output + c->output_used, data, length);
    c->output_used += length;
    return 0;
}

static int send_all(int fd, const char *message)
{
    for (int i = 0; i < MAX_CLIENTS; i++)
        if (clients[i].fd == fd) {
            if (queue_bytes(&clients[i], message, strlen(message)) == 0)
                return 0;
            shutdown(fd, SHUT_RDWR);
            return -1;
        }
    return -1;
}

static void notify_others(int excluded, const char *message)
{
    for (int i = 0; i < MAX_CLIENTS; i++)
        if (i != excluded && clients[i].fd >= 0 && clients[i].username[0] &&
            !clients[i].closing)
            send_all(clients[i].fd, message);
}

static void remove_client(int index)
{
    Client *c = &clients[index];
    if (c->fd < 0) return;
    char name[MAX_NAME + 1];
    strcpy(name, c->username);
    for (int r = 0; r < MAX_ROOMS; r++) rooms[r].members[index] = 0;
    if (c->upload) fclose(c->upload);
    if (c->temporary[0]) unlink(c->temporary);
    close(c->fd);
    free(c->output);
    memset(c, 0, sizeof(*c));
    c->fd = -1;
    if (name[0]) {
        char message[128];
        snprintf(message, sizeof(message), "MSG LEAVE %s\n", name);
        notify_others(index, message);
        printf("User left: %s\n", name);
    } else printf("Unregistered client disconnected\n");
    fflush(stdout);
}

static int ensure_directory(const char *path)
{
    if (mkdir(path, 0700) == 0) return 0;
    struct stat st;
    return errno == EEXIST && lstat(path, &st) == 0 && S_ISDIR(st.st_mode) ? 0 : -1;
}

static void finish_upload(int index)
{
    Client *c = &clients[index];
    unsigned char *bytes = NULL;
    if (c->upload && !c->upload_error) {
        if (fflush(c->upload) != 0 || fseek(c->upload, 0, SEEK_SET) != 0)
            c->upload_error = "ERR 007 STORAGE_FAILED" TAG;
        if (!c->upload_error && c->file_size) {
            bytes = malloc(c->file_size);
            if (!bytes || fread(bytes, 1, c->file_size, c->upload) != c->file_size)
                c->upload_error = "ERR 007 STORAGE_FAILED" TAG;
        }
    }
    if (c->upload) {
        if (fclose(c->upload) != 0 && !c->upload_error)
            c->upload_error = "ERR 007 STORAGE_FAILED" TAG;
        c->upload = NULL;
    }
    if (!c->upload_error && rename(c->temporary, c->destination) != 0)
        c->upload_error = "ERR 007 STORAGE_FAILED" TAG;
    if (c->upload_error) {
        if (c->temporary[0]) unlink(c->temporary);
        send_all(c->fd, c->upload_error);
    } else {
        char header[256];
        int h = snprintf(header, sizeof(header), "MSG FILE %s %s %zu\n",
                         c->username, c->filename, c->file_size);
        size_t frame_size = (size_t)h + c->file_size;
        unsigned char *frame = malloc(frame_size);
        int failed = !frame;
        if (frame) {
            memcpy(frame, header, (size_t)h);
            if (c->file_size) memcpy(frame + h, bytes, c->file_size);
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (!c->recipients[i]) continue;
                if (clients[i].fd < 0 || clients[i].generation != c->generations[i] ||
                    queue_bytes(&clients[i], frame, frame_size) < 0) {
                    failed = 1;
                    if (clients[i].fd >= 0 && clients[i].generation == c->generations[i])
                        shutdown(clients[i].fd, SHUT_RDWR);
                }
            }
            free(frame);
        }
        if (failed) send_all(c->fd, "ERR 007 DELIVERY_FAILED" TAG);
        else {
            char reply[256];
            snprintf(reply, sizeof(reply), "OK FILE_RECEIVED %s" TAG, c->filename);
            send_all(c->fd, reply);
        }
        printf("File stored: %s (%zu bytes)\n", c->destination, c->file_size);
        fflush(stdout);
    }
    free(bytes);
    c->receiving = 0;
    c->remaining = 0;
    c->temporary[0] = '\0';
    c->upload_error = NULL;
}

static void start_upload(int index, char *args)
{
    Client *c = &clients[index];
    char *target = args;
    char *filename = strchr(target, ' ');
    if (!filename) goto malformed;
    *filename++ = '\0';
    char *size_text = strchr(filename, ' ');
    if (!size_text) goto malformed;
    *size_text++ = '\0';
    if (!*size_text || size_text[strspn(size_text, "0123456789")]) goto malformed;
    errno = 0;
    char *end;
    uint64_t size = strtoull(size_text, &end, 10);
    if (errno || *end) goto malformed;
    c->remaining = size;
    c->receiving = 1;
    c->last_activity = time(NULL);
    c->file_size = size <= MAX_FILE ? (size_t)size : 0;
    c->upload_error = NULL;
    c->temporary[0] = '\0';
    memset(c->recipients, 0, sizeof(c->recipients));
    if (!c->username[0]) c->upload_error = "ERR 005 REGISTER_FIRST" TAG;
    else if (size > MAX_FILE) c->upload_error = "ERR 004 FILE_TOO_LARGE" TAG;
    else if (!valid_name(target) || !valid_filename(filename))
        c->upload_error = "ERR 005 INVALID_FORMAT" TAG;
    else {
        int user = -1;
        for (int i = 0; i < MAX_CLIENTS; i++)
            if (clients[i].fd >= 0 && !clients[i].closing &&
                strcmp(clients[i].username, target) == 0) { user = i; break; }
        int room = find_room(target);
        if (user >= 0) c->recipients[user] = 1;
        else if (room >= 0) {
            if (!rooms[room].members[index]) c->upload_error = "ERR 005 NOT_IN_ROOM" TAG;
            else for (int i = 0; i < MAX_CLIENTS; i++)
                if (i != index && rooms[room].members[i] && clients[i].fd >= 0 &&
                    !clients[i].closing) c->recipients[i] = 1;
        } else c->upload_error = "ERR 002 USER_OR_ROOM_NOT_FOUND" TAG;
        for (int i = 0; i < MAX_CLIENTS; i++) c->generations[i] = clients[i].generation;
    }
    if (!c->upload_error) {
        char directory[256];
        strcpy(c->filename, filename);
        snprintf(directory, sizeof(directory), STORAGE "/%s", c->username);
        snprintf(c->destination, sizeof(c->destination), "%s/%s", directory, filename);
        if (ensure_directory("storage") < 0 || ensure_directory(STORAGE) < 0 ||
            ensure_directory(directory) < 0) c->upload_error = "ERR 007 STORAGE_FAILED" TAG;
        else {
            snprintf(c->temporary, sizeof(c->temporary), "%s/.upload-XXXXXX", directory);
            int temp_fd = mkstemp(c->temporary);
            if (temp_fd < 0) c->upload_error = "ERR 007 STORAGE_FAILED" TAG;
            else {
                c->upload = fdopen(temp_fd, "w+b");
                if (!c->upload) { close(temp_fd); c->upload_error = "ERR 007 STORAGE_FAILED" TAG; }
            }
        }
    }
    /* A rejected header still owns exactly size payload bytes. Drain them
       before accepting another command, so binary bytes cannot become commands. */
    if (!size) finish_upload(index);
    return;
malformed:
    send_all(c->fd, "ERR 005 INVALID_FORMAT" TAG);
    c->closing = 1; /* Cannot safely recover without a trustworthy byte count. */
}

static void handle_command(int index, char *line)  {
    Client *c = &clients[index];
    char reply[MAX_LINE + MAX_NAME + 64];
    if (strcmp(line, "QUIT") == 0)  {
        send_all(c->fd, "OK BYE" TAG);
        c->closing = 1;
        return;
    }
    if (strncmp(line, "REGISTER ", 9) == 0)  {
        const char *name = line + 9;
        if (c->username[0] != '\0')  {
            send_all(c->fd, "ERR 005 ALREADY_REGISTERED" TAG);
            return;
        }
        if (!valid_name(name))  {
            send_all(c->fd, "ERR 005 INVALID_USERNAME" TAG);
            return;
        }
        for (int i = 0; i < MAX_CLIENTS; i++)  {
            if (clients[i].fd >= 0 && strcmp(clients[i].username, name) == 0)  {
                send_all(c->fd, "ERR 001 USERNAME_TAKEN" TAG);
                return;
            }
        }
        strcpy(c->username, name);
        snprintf(reply, sizeof(reply), "OK REGISTERED %s" TAG, name);
        if (send_all(c->fd, reply) < 0)  {
            remove_client(index);
            return;
        }
        snprintf(reply, sizeof(reply), "MSG JOIN %s\n", name);
        notify_others(index, reply);
        printf("User registered: %s\n", name);
        fflush(stdout);
        return;
    }
    if (strncmp(line, "SENDFILE ", 9) == 0)  {
        start_upload(index, line + 9);
        return;
    }
    if (c->username[0] == '\0')  {
        send_all(c->fd, "ERR 005 REGISTER_FIRST" TAG);
        return;
    }
    if (strcmp(line, "LIST") == 0)  {
        strcpy(reply, "OK USERS ");
        int first = 1;
        for (int i = 0; i < MAX_CLIENTS; i++)  {
            if (clients[i].fd >= 0 && clients[i].username[0] != '\0')  {
                if (!first) strcat(reply, ",");
                strcat(reply, clients[i].username);
                first = 0;
            }
        }
        strcat(reply, TAG);
        send_all(c->fd, reply);
        return;
    }
    if (strncmp(line, "BCAST ", 6) == 0)  {
        const char *message = line + 6;
        if (message[strspn(message, " \t")] == '\0')  {
            send_all(c->fd, "ERR 005 EMPTY_MESSAGE" TAG);
            return;
        }
        snprintf(reply, sizeof(reply), "MSG BCAST %s %s\n", c->username, message);
        notify_others(index, reply);
        send_all(c->fd, "OK SENT" TAG);
        return;
    }
    if (strncmp(line, "PMSG ", 5) == 0)  {
        char *target = line + 5;
        char *separator = strchr(target, ' ');
        if (separator == NULL)  {
            send_all(c->fd, "ERR 005 INVALID_FORMAT" TAG);
            return;
        }
        *separator = '\0';
        const char *message = separator + 1;
        if (!valid_name(target))  {
            send_all(c->fd, "ERR 005 INVALID_USERNAME" TAG);
            return;
        }
        if (message[strspn(message, " \t")] == '\0')  {
            send_all(c->fd, "ERR 005 EMPTY_MESSAGE" TAG);
            return;
        }
        int recipient = -1;
        for (int i = 0; i < MAX_CLIENTS; i++)  {
            if (clients[i].fd >= 0 && clients[i].username[0] != '\0' && strcmp(clients[i].username, target) == 0)  {
                recipient = i;
                break;
            }
        }
        if (recipient < 0)  {
            send_all(c->fd, "ERR 002 USER_NOT_FOUND" TAG);
            return;
        }
        snprintf(reply, sizeof(reply), "MSG PRIV %s %s\n", c->username, message);
        if (send_all(clients[recipient].fd, reply) < 0) send_all(c->fd, "ERR 007 DELIVERY_FAILED" TAG);
        else send_all(c->fd, "OK SENT" TAG);
        return;
    }
    if (strncmp(line, "JOIN ", 5) == 0)  {
        const char *name = line + 5;
        if (!valid_name(name))  {
            send_all(c->fd, "ERR 005 INVALID_ROOM_NAME" TAG);
            return;
        }
        int room = find_room(name);
        if (room < 0)  {
            for (int i = 0; i < MAX_ROOMS; i++)  {
                if (rooms[i].name[0] == '\0')  {
                    room = i;
                    strcpy(rooms[i].name, name);
                    break;
                }
            }
        }
        if (room < 0)  {
            send_all(c->fd, "ERR 006 ROOM_LIMIT_REACHED" TAG);
            return;
        }
        rooms[room].members[index] = 1;
        snprintf(reply, sizeof(reply), "OK JOINED %s" TAG, name);
        send_all(c->fd, reply);
        return;
    }
    if (strcmp(line, "ROOMS") == 0)  {
        strcpy(reply, "OK ROOMS ");
        int first = 1;
        for (int i = 0; i < MAX_ROOMS; i++)  {
            if (rooms[i].name[0] != '\0')  {
                if (!first) strcat(reply, ",");
                strcat(reply, rooms[i].name);
                first = 0;
            }
        }
        strcat(reply, TAG);
        send_all(c->fd, reply);
        return;
    }
    if (strncmp(line, "LEAVE ", 6) == 0)  {
        const char *name = line + 6;
        if (!valid_name(name))  {
            send_all(c->fd, "ERR 005 INVALID_ROOM_NAME" TAG);
            return;
        }
        int room = find_room(name);
        if (room < 0)  {
            send_all(c->fd, "ERR 003 ROOM_NOT_FOUND" TAG);
            return;
        }
        if (!rooms[room].members[index])  {
            send_all(c->fd, "ERR 005 NOT_IN_ROOM" TAG);
            return;
        }
        rooms[room].members[index] = 0;
        snprintf(reply, sizeof(reply), "OK LEFT %s" TAG, name);
        send_all(c->fd, reply);
        return;
    }
    if (strncmp(line, "RMSG ", 5) == 0)  {
        char *name = line + 5;
        char *separator = strchr(name, ' ');
        if (separator == NULL)  {
            send_all(c->fd, "ERR 005 INVALID_FORMAT" TAG);
            return;
        }
        *separator = '\0';
        const char *message = separator + 1;
        if (!valid_name(name))  {
            send_all(c->fd, "ERR 005 INVALID_ROOM_NAME" TAG);
            return;
        }
        if (message[strspn(message, " \t")] == '\0')  {
            send_all(c->fd, "ERR 005 EMPTY_MESSAGE" TAG);
            return;
        }
        int room = find_room(name);
        if (room < 0)  {
            send_all(c->fd, "ERR 003 ROOM_NOT_FOUND" TAG);
            return;
        }
        if (!rooms[room].members[index])  {
            send_all(c->fd, "ERR 005 NOT_IN_ROOM" TAG);
            return;
        }
        snprintf(reply, sizeof(reply), "MSG ROOM %s %s %s\n", name, c->username, message);
        for (int i = 0; i < MAX_CLIENTS; i++)  {
            if (i != index && rooms[room].members[i] && clients[i].fd >= 0 && clients[i].username[0] != '\0') send_all(clients[i].fd, reply);
        }
        send_all(c->fd, "OK SENT" TAG);
        return;
    }
    send_all(c->fd, "ERR 005 INVALID_COMMAND" TAG);
}

static void read_client(int index)
{
    Client *c = &clients[index];
    unsigned char buffer[4096];
    ssize_t n = recv(c->fd, buffer, sizeof(buffer), 0);
    if (n < 0) {
        if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) return;
        remove_client(index);
        return;
    }
    if (!n) { remove_client(index); return; }
    c->last_activity = time(NULL);
    size_t i = 0;
    while (i < (size_t)n && c->fd >= 0 && !c->closing) {
        if (c->receiving) {
            size_t take = (size_t)n - i;
            if ((uint64_t)take > c->remaining) take = (size_t)c->remaining;
            if (c->upload && !c->upload_error &&
                fwrite(buffer + i, 1, take, c->upload) != take)
                c->upload_error = "ERR 007 STORAGE_FAILED" TAG;
            c->remaining -= take;
            i += take;
            if (!c->remaining) finish_upload(index);
            continue;
        }
        unsigned char ch = buffer[i++];
        if (ch == '\0') {
            send_all(c->fd, "ERR 005 INVALID_TEXT" TAG);
            c->closing = 1;
        } else if (ch == '\n') {
            if (c->used && c->input[c->used - 1] == '\r') c->used--;
            c->input[c->used] = '\0';
            handle_command(index, c->input);
            c->used = 0;
        } else if (c->used >= sizeof(c->input) - 1) {
            send_all(c->fd, "ERR 005 LINE_TOO_LONG" TAG);
            c->closing = 1;
        } else c->input[c->used++] = (char)ch;
    }
}

static void flush_output(int index)
{
    Client *c = &clients[index];
    if (!c->output_used) return;
    ssize_t n = send(c->fd, c->output, c->output_used, 0);
    if (n < 0) {
        if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) return;
        remove_client(index);
    } else if (!n) remove_client(index);
    else {
        c->output_used -= (size_t)n;
        if (c->output_used) memmove(c->output, c->output + n, c->output_used);
        else { free(c->output); c->output = NULL; }
    }
}

int main(void)
{
    signal(SIGPIPE, SIG_IGN);
    for (int i = 0; i < MAX_CLIENTS; i++) clients[i].fd = -1;
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); return EXIT_FAILURE; }
    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        perror("setsockopt"); close(server_fd); return EXIT_FAILURE;
    }
    struct sockaddr_in address = {0};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(PORT);
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0 ||
        listen(server_fd, MAX_CLIENTS) < 0) {
        perror("bind/listen"); close(server_fd); return EXIT_FAILURE;
    }
    printf("NetMessenger server - IT23620452\nListening on port %d | NID:6204\n", PORT);
    printf("Commands: REGISTER, LIST, BCAST, PMSG, JOIN, LEAVE, ROOMS, RMSG, SENDFILE, QUIT\n");
    printf("Maximum file size: %u bytes\n", MAX_FILE);
    fflush(stdout);
    for (;;) {
        struct pollfd fds[MAX_CLIENTS + 1];
        fds[0] = (struct pollfd){server_fd, POLLIN, 0};
        for (int i = 0; i < MAX_CLIENTS; i++)
            fds[i + 1] = (struct pollfd){clients[i].fd,
                (short)((clients[i].closing ? 0 : POLLIN) |
                        (clients[i].output_used ? POLLOUT : 0)), 0};
        if (poll(fds, MAX_CLIENTS + 1, 1000) < 0) {
            if (errno == EINTR) continue;
            perror("poll"); break;
        }
        for (int i = 0; i < MAX_CLIENTS; i++) {
            Client *c = &clients[i];
            short events = fds[i + 1].revents;
            if (c->fd < 0) continue;
            if (events & POLLIN) read_client(i);
            if (c->fd >= 0 && (events & POLLOUT)) flush_output(i);
            if (c->fd >= 0 && (events & (POLLERR | POLLNVAL))) remove_client(i);
            if (c->fd >= 0 && (events & POLLHUP) && !(events & POLLIN)) remove_client(i);
            if (c->fd >= 0 && c->receiving && time(NULL) - c->last_activity >= 30) {
                send_all(c->fd, "ERR 007 UPLOAD_TIMEOUT" TAG);
                c->closing = 1;
            }
            if (c->fd >= 0 && c->closing && !c->output_used) remove_client(i);
        }
        if (fds[0].revents & POLLIN) {
            int fd = accept(server_fd, NULL, NULL);
            if (fd < 0) { if (errno != EINTR) perror("accept"); continue; }
            int flags = fcntl(fd, F_GETFL, 0);
            if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
                perror("nonblocking socket"); close(fd); continue;
            }
            int slot = -1;
            for (int i = 0; i < MAX_CLIENTS; i++)
                if (clients[i].fd < 0) { slot = i; break; }
            if (slot < 0) {
                const char *reply = "ERR 006 SERVER_FULL" TAG;
                (void)send(fd, reply, strlen(reply), 0);
                close(fd);
            } else {
                memset(&clients[slot], 0, sizeof(clients[slot]));
                clients[slot].fd = fd;
                clients[slot].generation = ++next_generation;
                clients[slot].last_activity = time(NULL);
                printf("Client connected; waiting for REGISTER\n");
                fflush(stdout);
            }
        }
    }
    for (int i = 0; i < MAX_CLIENTS; i++) remove_client(i);
    close(server_fd);
    return EXIT_FAILURE;
}
