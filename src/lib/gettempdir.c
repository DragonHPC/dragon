/*
 * C equivalent of Python's tempfile.gettempdir() for Linux and macOS.
 *
 * Keep in sync with CPython's tempfile._candidate_tempdir_list() and
 * tempfile._get_default_tempdir(); test/ext_attach compares the two.
 */
#include <dragon/utils.h>
#include "err.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define _TEMPDIR_NAME_CHARS "abcdefghijklmnopqrstuvwxyz0123456789_"
#define _TEMPDIR_NAME_LEN 8
#define _TEMPDIR_TRIES_PER_DIR 100

static const char* _tempdir_env_vars[] = {"TMPDIR", "TEMP", "TMP"};

/* CPython has no macOS-specific candidates; the per-user macOS temp dir arrives through TMPDIR. */
static const char* _tempdir_fallbacks[] = {"/tmp", "/var/tmp", "/usr/tmp"};

static uint64_t
_tempdir_next_random(uint64_t* state)
{
    uint64_t x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    return x;
}

/* Equivalent of os.path.abspath(): join with the cwd if relative, then posixpath.normpath(). */
static char*
_tempdir_abspath(const char* path)
{
    char* joined = NULL;

    if (path[0] == '/') {
        joined = strdup(path);
    } else {
        char cwd[PATH_MAX];
        if (getcwd(cwd, sizeof(cwd)) == NULL)
            return NULL;
        size_t cwd_len = strlen(cwd);
        joined = malloc(cwd_len + 1 + strlen(path) + 1);
        if (joined != NULL) {
            strcpy(joined, cwd);
            if (cwd_len == 0 || cwd[cwd_len - 1] != '/')
                strcat(joined, "/");
            strcat(joined, path);
        }
    }

    if (joined == NULL)
        return NULL;

    size_t len = strlen(joined);
    char* out = malloc(len + 1);
    if (out == NULL) {
        free(joined);
        return NULL;
    }

    /* posixpath.normpath keeps exactly two leading slashes, but collapses three or more to one. */
    size_t base = (joined[1] == '/' && joined[2] != '/') ? 2 : 1;
    memset(out, '/', base);
    size_t out_len = base;

    char* saveptr = NULL;
    for (char* comp = strtok_r(joined, "/", &saveptr); comp != NULL; comp = strtok_r(NULL, "/", &saveptr)) {
        if (strcmp(comp, ".") == 0)
            continue;

        if (strcmp(comp, "..") == 0) {
            while (out_len > base && out[out_len - 1] != '/')
                out_len--;
            if (out_len > base)
                out_len--;
            continue;
        }

        if (out_len > base)
            out[out_len++] = '/';
        size_t comp_len = strlen(comp);
        memcpy(out + out_len, comp, comp_len);
        out_len += comp_len;
    }

    out[out_len] = '\0';
    free(joined);
    return out;
}

/* Mirrors the create/write/unlink probe in tempfile._get_default_tempdir(). */
static bool
_tempdir_is_usable(const char* dir, uint64_t* rng)
{
    const char* chars = _TEMPDIR_NAME_CHARS;
    size_t num_chars = strlen(chars);
    size_t dir_len = strlen(dir);
    bool needs_sep = dir_len > 0 && dir[dir_len - 1] != '/';
    char* filename = malloc(dir_len + 1 + _TEMPDIR_NAME_LEN + 1);
    bool usable = false;

    if (filename == NULL)
        return false;

    strcpy(filename, dir);
    char* name = filename + dir_len;
    if (needs_sep)
        *name++ = '/';
    name[_TEMPDIR_NAME_LEN] = '\0';

    for (int seq = 0; seq < _TEMPDIR_TRIES_PER_DIR; seq++) {
        for (int idx = 0; idx < _TEMPDIR_NAME_LEN; idx++)
            name[idx] = chars[_tempdir_next_random(rng) % num_chars];

        int fd = open(filename, O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600);
        if (fd >= 0) {
            ssize_t written = write(fd, "blat", 4);
            int close_rc = close(fd);
            int unlink_rc = unlink(filename);
            usable = written >= 0 && close_rc == 0 && unlink_rc == 0;
            break;
        }

        if (errno != EEXIST)
            break;
    }

    free(filename);
    return usable;
}

static bool
_tempdir_try_candidate(const char* candidate, uint64_t* rng, char** tmpdir)
{
    char* dir = _tempdir_abspath(candidate);
    if (dir == NULL)
        return false;

    if (_tempdir_is_usable(dir, rng)) {
        *tmpdir = dir;
        return true;
    }

    free(dir);
    return false;
}

/**
 * @brief Find the default directory for temporary files, like Python's tempfile.gettempdir().
 *
 * Candidates are tried in the same order as CPython on Linux and macOS: the TMPDIR,
 * TEMP and TMP environment variables (skipping unset or empty ones), then /tmp,
 * /var/tmp, /usr/tmp, and finally the current working directory. The first candidate
 * in which a file can be created, written, and removed is returned as an absolute,
 * normalized path. Unlike Python, the result is not cached between calls.
 *
 * @param tmpdir Set to a newly allocated path string that the caller must free().
 * @return DRAGON_SUCCESS, or DRAGON_NOT_FOUND if no usable directory exists.
 */
dragonError_t
dragon_gettempdir(char** tmpdir)
{
    if (tmpdir == NULL)
        err_return(DRAGON_INVALID_ARGUMENT, "The tmpdir argument cannot be NULL.");

    *tmpdir = NULL;
    uint64_t rng = ((uint64_t)time(NULL) << 32) ^ (uint64_t)getpid() ^ (uint64_t)(uintptr_t)tmpdir;
    if (rng == 0)
        rng = 0x9E3779B97F4A7C15ULL;

    for (size_t idx = 0; idx < sizeof(_tempdir_env_vars) / sizeof(_tempdir_env_vars[0]); idx++) {
        const char* value = getenv(_tempdir_env_vars[idx]);
        if (value != NULL && value[0] != '\0' && _tempdir_try_candidate(value, &rng, tmpdir))
            no_err_return(DRAGON_SUCCESS);
    }

    for (size_t idx = 0; idx < sizeof(_tempdir_fallbacks) / sizeof(_tempdir_fallbacks[0]); idx++) {
        if (_tempdir_try_candidate(_tempdir_fallbacks[idx], &rng, tmpdir))
            no_err_return(DRAGON_SUCCESS);
    }

    char cwd[PATH_MAX];
    if (getcwd(cwd, sizeof(cwd)) != NULL) {
        if (_tempdir_try_candidate(cwd, &rng, tmpdir))
            no_err_return(DRAGON_SUCCESS);
    } else if (_tempdir_is_usable(".", &rng)) {
        /* Python falls back to os.curdir without making it absolute. */
        *tmpdir = strdup(".");
        if (*tmpdir == NULL)
            err_return(DRAGON_INTERNAL_MALLOC_FAIL, "Could not allocate the temporary directory path.");
        no_err_return(DRAGON_SUCCESS);
    }

    err_return(DRAGON_NOT_FOUND, "No usable temporary directory found.");
}
