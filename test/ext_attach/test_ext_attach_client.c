/*
 * Standalone C test client for the dragon_ext_attach()/dragon_ext_detach()
 * bootstrap API (see src/include/dragon/ext_attach.h). No Python involved.
 *
 * Usage: test_ext_attach_client <mode>
 *   attach_detach  - attach with a correct token, print p_uid, detach cleanly
 *   bad_token      - attach with a wrong token; expect AUTH_FAILURE
 *   double_detach  - attach, detach, detach again; expect the 2nd to fail
 *   reattach       - attach, detach, attach again; both must succeed
 *   stale_runtime  - point DRAGON_ATTACH_DIR at a dir with no attach_info.txt
 *   gettempdir     - print the result of dragon_gettempdir()
 */

#include <dragon/ext_attach.h>
#include <dragon/utils.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int
mode_attach_detach(void)
{
    dragonExtAttachDescr_t handle;
    dragonExtRegisterErr_t reg_err = DRAGON_EXT_REGISTER_SUCCESS;
    dragonError_t err = dragon_ext_attach(NULL, NULL, &reg_err, &handle);
    if (err != DRAGON_SUCCESS) {
        fprintf(stderr, "attach_detach: dragon_ext_attach failed: %s\n", dragon_get_rc_string(err));
        return 1;
    }

    dragonP_UID_t puid = 0;
    err = dragon_ext_get_puid(&handle, &puid);
    if (err != DRAGON_SUCCESS) {
        fprintf(stderr, "attach_detach: dragon_ext_get_puid failed: %s\n", dragon_get_rc_string(err));
        return 1;
    }
    printf("p_uid=%lu\n", (unsigned long)puid);
    fflush(stdout);

    usleep(200000);

    dragonExtDetachErr_t detach_err = DRAGON_EXT_DETACH_SUCCESS;
    err = dragon_ext_detach(&handle, NULL, &detach_err);
    if (err != DRAGON_SUCCESS) {
        fprintf(stderr, "attach_detach: dragon_ext_detach failed: %s\n", dragon_get_rc_string(err));
        return 1;
    }

    return 0;
}

static int
mode_bad_token(void)
{
    dragonExtAttachDescr_t handle;
    dragonExtRegisterErr_t reg_err = DRAGON_EXT_REGISTER_SUCCESS;
    dragonError_t err = dragon_ext_attach("definitely-the-wrong-token", NULL, &reg_err, &handle);

    if (err != DRAGON_INVALID_OPERATION) {
        fprintf(stderr, "bad_token: expected DRAGON_INVALID_OPERATION, got %s\n", dragon_get_rc_string(err));
        return 1;
    }

    if (reg_err != DRAGON_EXT_REGISTER_AUTH_FAILURE) {
        fprintf(stderr, "bad_token: expected AUTH_FAILURE, got %d\n", (int)reg_err);
        return 1;
    }

    return 0;
}

static int
mode_double_detach(void)
{
    dragonExtAttachDescr_t handle;
    dragonExtRegisterErr_t reg_err = DRAGON_EXT_REGISTER_SUCCESS;
    dragonError_t err = dragon_ext_attach(NULL, NULL, &reg_err, &handle);
    if (err != DRAGON_SUCCESS) {
        fprintf(stderr, "double_detach: attach failed: %s\n", dragon_get_rc_string(err));
        return 1;
    }

    err = dragon_ext_detach(&handle, NULL, NULL);
    if (err != DRAGON_SUCCESS) {
        fprintf(stderr, "double_detach: first detach failed: %s\n", dragon_get_rc_string(err));
        return 1;
    }

    err = dragon_ext_detach(&handle, NULL, NULL);
    if (err != DRAGON_INVALID_ARGUMENT) {
        fprintf(stderr, "double_detach: expected DRAGON_INVALID_ARGUMENT on 2nd detach, got %s\n",
                dragon_get_rc_string(err));
        return 1;
    }

    return 0;
}

static int
mode_reattach(void)
{
    dragonExtAttachDescr_t handle1, handle2;
    dragonExtRegisterErr_t reg_err = DRAGON_EXT_REGISTER_SUCCESS;

    dragonError_t err = dragon_ext_attach(NULL, NULL, &reg_err, &handle1);
    if (err != DRAGON_SUCCESS) {
        fprintf(stderr, "reattach: first attach failed: %s\n", dragon_get_rc_string(err));
        return 1;
    }

    err = dragon_ext_detach(&handle1, NULL, NULL);
    if (err != DRAGON_SUCCESS) {
        fprintf(stderr, "reattach: detach failed: %s\n", dragon_get_rc_string(err));
        return 1;
    }

    err = dragon_ext_attach(NULL, NULL, &reg_err, &handle2);
    if (err != DRAGON_SUCCESS) {
        fprintf(stderr, "reattach: second attach failed: %s\n", dragon_get_rc_string(err));
        return 1;
    }

    err = dragon_ext_detach(&handle2, NULL, NULL);
    if (err != DRAGON_SUCCESS) {
        fprintf(stderr, "reattach: second detach failed: %s\n", dragon_get_rc_string(err));
        return 1;
    }

    return 0;
}

static int
mode_stale_runtime(void)
{
    /* Caller is expected to have set DRAGON_ATTACH_DIR to point at a
       directory with no attach_info.txt before running this mode. */
    dragonExtAttachDescr_t handle;
    dragonExtRegisterErr_t reg_err = DRAGON_EXT_REGISTER_SUCCESS;
    dragonError_t err = dragon_ext_attach(NULL, NULL, &reg_err, &handle);

    if (err != DRAGON_NOT_FOUND) {
        fprintf(stderr, "stale_runtime: expected DRAGON_NOT_FOUND, got %s\n", dragon_get_rc_string(err));
        return 1;
    }

    return 0;
}

static int
mode_gettempdir(void)
{
    char* tmpdir = NULL;
    dragonError_t err = dragon_gettempdir(&tmpdir);
    if (err != DRAGON_SUCCESS) {
        fprintf(stderr, "gettempdir: dragon_gettempdir failed: %s\n", dragon_get_rc_string(err));
        return 1;
    }

    printf("%s\n", tmpdir);
    free(tmpdir);
    return 0;
}

int
main(int argc, char** argv)
{
    if (argc != 2) {
        fprintf(stderr, "usage: %s <mode>\n", argv[0]);
        return 1;
    }

    const char* mode = argv[1];

    if (strcmp(mode, "attach_detach") == 0)
        return mode_attach_detach();
    if (strcmp(mode, "bad_token") == 0)
        return mode_bad_token();
    if (strcmp(mode, "double_detach") == 0)
        return mode_double_detach();
    if (strcmp(mode, "reattach") == 0)
        return mode_reattach();
    if (strcmp(mode, "stale_runtime") == 0)
        return mode_stale_runtime();
    if (strcmp(mode, "gettempdir") == 0)
        return mode_gettempdir();

    fprintf(stderr, "unknown mode: %s\n", mode);
    return 1;
}
