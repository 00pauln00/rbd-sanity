/* rbd-sanity.c
 * Tool for writing verifiable contents to a Ceph RBD volume.
 *
 * Paul Nowoczynski (pnowocyznski@digitalocean.com)
 */

#define _GNU_SOURCE 1 // for O_DIRECT
#include <libaio.h>
#include <rbd/librbd.h>
#include <getopt.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <pthread.h>

#ifndef NBBY
#define NBBY 8
#endif

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

#define OPTS "b:c:d:D:f:i:hm:N:n:o:p:Rs:Vvz:"

#define DEF_OUTPUT_FILE          "qos-rbd-tester.out"
#define CONF_DEFAULT             "/etc/ceph/ceph.conf"
#define RBD_IMAGE_DEFAULT        "qos-rbd-tester"
#define CLIENT_NAME_DEFAULT      "admin"
#define POOL_DEFAULT             "rbd"
#define MAX_IODEPTH              512
#define DEF_IODEPTH              32
#define MAX_OVERWRITES           512
#define DEF_OVERWRITES           11
#define DEF_RUNTIME_SECS         30
#define DEF_BLOCK_SIZE           4096
#define MAX_BLOCK_SIZE           1073741824
#define ENABLE_CONCURRENT_READS  0
#define DEF_MULTI_BLOCK_IO       0
#define DEF_FILE_IO_SIZE         1073741824

#define VERSION_MAJOR            1
#define VERSION_MINOR            0

// modulo divisor for overriding some portion of the randomly calculated
// iodepths.  A lower value will increase the probability of the iodepth being
// forced to '1'.
#define QRT_FORCE_IODEPTH_OF_ONE 2

enum log_levels
{
    LL_FATAL = 0,
    LL_ERROR = 1,
    LL_WARN  = 2,
    LL_DEBUG = 3,
    LL_TRACE = 4,
    LL_MAX
};

static const char *
ll_to_string(int level)
{
    switch (level)
    {
    case LL_FATAL:
        return "fatal";
    case LL_ERROR:
        return "error";
    case LL_WARN:
        return "warn";
    case LL_DEBUG:
        return "debug";
    case LL_TRACE:
    default:
        return "trace";
        break;
    }
    return "debug";
}

#define log_msg(lvl, message, ...)                                      \
{                                                                       \
    if (lvl <= qrtLogLevel)                                             \
    {                                                                   \
        fprintf(stderr, "<%s:%s:%u> " message "\n",                     \
                ll_to_string(lvl), __func__,                            \
                __LINE__, ##__VA_ARGS__);                               \
        if (lvl == LL_FATAL)                                            \
            exit(1);                                                    \
    }                                                                   \
}

#define NUM_READ_THREADS                         1
#define MAX_READ_THREAD_SLEEP_PERCENTAGE_RUNTIME 10
#define MAX_READ_THREAD_SLEEP_SECS               10

#ifdef DEBUG
static const unsigned long long qrtBlockMagicWord = 0x0;
#else
static const unsigned long long qrtBlockMagicWord = 0xabcdefabcdef0000;
#endif
static char                    *qrtExecName;
static enum log_levels          qrtLogLevel       = LL_WARN;
static int                      qrtRuntime        = DEF_RUNTIME_SECS;
static bool                     qrtVerifyMode     = false;
static bool                     qrtEnableReadThr  = ENABLE_CONCURRENT_READS;
static pthread_t                qrtReadThreads[NUM_READ_THREADS];

#define RANDOM_STATE_BUF_LEN 32
static __thread unsigned long long **qrtBufs;
static __thread bool                 qrtRandInit;
static __thread struct random_data   qrtRandomData;
static __thread char                 qrtRandomStateBuf[RANDOM_STATE_BUF_LEN];
static __thread io_context_t         qrtAioCtx;

struct my_rbd_info
{
    rados_t          rbdi_cluster;
    rados_ioctx_t    rbdi_io_ctx;
    rbd_image_t      rbdi_image;
    rbd_image_info_t rbdi_stat;
};

struct my_file_info
{
    const char *fi_output_file_name;
    int         fi_fd;
    off_t       fi_size;
};

typedef unsigned short output_val_t;

#define MAX_OVERWRITES_TOLERATED (1ULL << sizeof(output_val_t) * NBBY)

// 'output_info' is written (via mmap / msync) to the output file and used
// for RBD volume verification.
struct output_info
{
    output_val_t last_write_val;
} __attribute__((packed));

struct qrt_config
{
    bool                 qrtc_file_io;
    union {
        struct my_rbd_info  qrtc_rbd_info;
        struct my_file_info qrtc_fi_info;
    };
    struct output_info  *qrtc_output;
    int                  qrtc_output_fd;
    int                  qrtc_num_overwrites;
    char                *qrtc_pool_name;
    char                *qrtc_image_name;
    char                *qrtc_client_name;
    char                *qrtc_conf_file;
    char                *qrtc_output_file;
    size_t               qrtc_block_size;
    size_t               qrtc_io_depth;
    size_t               qrtc_multi_block;
};

static void
qrt_config_init(struct qrt_config *qrtc)
{
    memset(qrtc, 0, sizeof(struct qrt_config));

    // Set up the defaults (this may be overridden in getopt()).
    qrtc->qrtc_pool_name      = POOL_DEFAULT;
    qrtc->qrtc_image_name     = RBD_IMAGE_DEFAULT;
    qrtc->qrtc_client_name    = CLIENT_NAME_DEFAULT;
    qrtc->qrtc_conf_file      = CONF_DEFAULT;
    qrtc->qrtc_output_file    = DEF_OUTPUT_FILE;
    qrtc->qrtc_num_overwrites = DEF_OVERWRITES;
    qrtc->qrtc_block_size     = DEF_BLOCK_SIZE;
    qrtc->qrtc_io_depth       = DEF_IODEPTH;
    qrtc->qrtc_output_fd      = -1;
    qrtc->qrtc_multi_block    = DEF_MULTI_BLOCK_IO;
}

struct my_rbd_info *
qrtc_get_rbd_info(const struct qrt_config *qrtc)
{
    return &((struct qrt_config *)qrtc)->qrtc_rbd_info;
}

static void
qrt_print_help(const int exit_val)
{
    fprintf(exit_val ? stderr : stdout,
            "%s [OPTION]\n\n"
            "Options:\n"
            "\t-b  Block Size\t\t(default: %d, max: %d)\n"
            "\t-c  Conf File\t\t(default: '%s')\n"
            "\t-D  Debug Level\t\t[%d -> %d (most verbose)]\n"
            "\t-d  IO Depth\t\t(default: %d, max: %d)\n"
            "\t-f  Use file backend instead of RBD\n"
            "\t-i  RBD Image Name\t(default: '%s')\n"
            "\t-h  Help\n"
            "\t-m  Multi-block I/O\t(default: '%d')\n"
            "\t-n  Client Name\t\t(default: '%s')\n"
            "\t-N  Max Number of Block Overwrites\t(default: %d)\n"
            "\t-o  Output File\t\t(default: '%s')\n"
            "\t-p  Pool Name\t\t(default: '%s')\n"
            "\t-R  Enable concurrent read thread (default: '%d')\n"
            "\t-s  Runtime\t\t(default: '%d seconds')\n"
            "\t-V  Verify Mode\n\n",
            qrtExecName, DEF_BLOCK_SIZE, MAX_BLOCK_SIZE, CONF_DEFAULT,
            LL_FATAL, LL_TRACE, DEF_IODEPTH, MAX_IODEPTH, RBD_IMAGE_DEFAULT,
            DEF_MULTI_BLOCK_IO,
            CLIENT_NAME_DEFAULT, DEF_OVERWRITES, DEF_OUTPUT_FILE, POOL_DEFAULT,
            ENABLE_CONCURRENT_READS, DEF_RUNTIME_SECS);

    exit(exit_val);
}

static void
qrt_print_version(void)
{
    fprintf(stdout, "%s %d.%d\n", qrtExecName,
            VERSION_MAJOR, VERSION_MINOR);
    exit(0);
}

static void
qrt_getopts(int argc, char **argv, struct qrt_config *qrtc)
{
    int opt;

    qrtExecName = argv[0];

    while ((opt = getopt(argc, argv, OPTS)) != -1)
    {
        switch (opt)
        {
        case 'b':
            qrtc->qrtc_block_size = strtoull(optarg, NULL, 10);
            break;
        case 'c':
            qrtc->qrtc_conf_file = optarg;
            break;
        case 'D':
            qrtLogLevel = atoi(optarg);
            break;
        case 'd':
            qrtc->qrtc_io_depth = strtoull(optarg, NULL, 10);
            break;
        case 'f':
            qrtc->qrtc_file_io = true;
            qrtc->qrtc_fi_info.fi_output_file_name = optarg;
            break;
        case 'h':
            qrt_print_help(0);
            break;
        case 'i':
            qrtc->qrtc_image_name = optarg;
            break;
        case 'm':
            qrtc->qrtc_multi_block = atoi(optarg);
            break;
        case 'N':
            qrtc->qrtc_num_overwrites = MIN(MAX_OVERWRITES, atoi(optarg));
            break;
        case 'n':
            qrtc->qrtc_client_name = optarg;
            break;
        case 'o':
            qrtc->qrtc_output_file = optarg;
            break;
        case 'p':
            qrtc->qrtc_pool_name = optarg;
            break;
        case 's':
            qrtRuntime = atoi(optarg);
            break;
        case 'R':
            qrtEnableReadThr = true;
            break;
        case 'V':
            qrtVerifyMode = true;
            break;
        case 'v':
            qrt_print_version();
            break;
        case 'z':
            qrtc->qrtc_fi_info.fi_size = strtoull(optarg, NULL, 10);
            break;
        default:
            qrt_print_help(1);
            break;
        }
    }
}

static unsigned int
qrt_get_random(void)
{
    if (!qrtRandInit)
    {
        if (initstate_r(0, qrtRandomStateBuf, RANDOM_STATE_BUF_LEN,
                        &qrtRandomData))
            log_msg(LL_FATAL, "initstate_r() failed: %s", strerror(errno));

        qrtRandInit = true;
    }

    unsigned int result;
    if (random_r(&qrtRandomData, (int *)&result))
        log_msg(LL_FATAL, "random_r() failed: %s", strerror(errno));

    return result;
}

static void
qrt_assert_output_value_bounds(size_t io_depth, size_t num_overwrites)
{
    if ((io_depth * num_overwrites) > MAX_OVERWRITES_TOLERATED)
        log_msg(LL_FATAL, "(io_depth (%zu) * num_overwrites (%zu)) "
                "exceeds the stored overwrite counter size (%llu)",
                io_depth, num_overwrites, MAX_OVERWRITES_TOLERATED);
}

static void
qrt_config_check(const struct qrt_config *qrtc)
{
    if (qrtc->qrtc_io_depth > MAX_IODEPTH)
        log_msg(LL_FATAL, "io_depth must be <= %d", MAX_IODEPTH);

    if (qrtc->qrtc_block_size % sizeof(unsigned long long))
        log_msg(LL_FATAL, "block size must be multiple of %zd",
                sizeof(unsigned long long));

    qrt_assert_output_value_bounds(qrtc->qrtc_io_depth,
                                   qrtc->qrtc_num_overwrites);
}

static inline size_t
qrtc_config_words_per_block(const struct qrt_config *qrtc)
{
    return (size_t)(qrtc->qrtc_block_size / sizeof(unsigned long long));
}

static inline size_t
qrtc_get_num_outputs(const struct qrt_config *qrtc)
{
    return (size_t)(qrtc->qrtc_file_io ?
                    (size_t)qrtc->qrtc_fi_info.fi_size :
                    qrtc_get_rbd_info(qrtc)->rbdi_stat.size) /
        qrtc->qrtc_block_size;
}

static inline size_t
qrtc_get_output_size(const struct qrt_config *qrtc)
{
    return (size_t)(qrtc_get_num_outputs(qrtc) * sizeof(struct output_info));
}

static int
qrtc_open_and_mmap_output_file(struct qrt_config *qrtc)
{
    int rc = 0;

    qrtc->qrtc_output_fd =
        open(qrtc->qrtc_output_file,
             qrtVerifyMode ? O_RDONLY : (O_RDWR | O_CREAT | O_TRUNC), 0775);

    if (qrtc->qrtc_output_fd < 0)
    {
        rc = errno;
        log_msg(LL_ERROR, "open(%s, ..) failed: %s",
                qrtc->qrtc_output_file, strerror(rc));

        return -rc;
    }

    if (!qrtVerifyMode)
        if (ftruncate(qrtc->qrtc_output_fd, (off_t)qrtc_get_output_size(qrtc)))
        {
            rc = errno;
            log_msg(LL_ERROR, "ftruncate() failed: %s", strerror(rc));

            return -rc;
        }

    qrtc->qrtc_output =
        mmap(NULL, qrtc_get_output_size(qrtc),
             qrtVerifyMode ? PROT_READ : PROT_WRITE, MAP_SHARED,
             qrtc->qrtc_output_fd, 0);

    if ((ssize_t)qrtc->qrtc_output == -1)
    {
        rc = errno;
        log_msg(LL_ERROR, "mmap() failed: %s", strerror(rc));
    }

    return -rc;
}

static void
qrt_rbd_disconnect(struct my_rbd_info *rbd_info)
{
    if (rbd_info->rbdi_image)
        rbd_close(rbd_info->rbdi_image);

    if (rbd_info->rbdi_io_ctx)
        rados_ioctx_destroy(rbd_info->rbdi_io_ctx);

    if (rbd_info->rbdi_cluster)
        rados_shutdown(rbd_info->rbdi_cluster);
}

static int
qrtc_aio_init(const struct qrt_config *qrtc)
{
    if (!qrtc->qrtc_file_io)
        log_msg(LL_FATAL, "aio is not configured");

    int rc = io_setup(qrtc->qrtc_io_depth, &qrtAioCtx);
    if (rc)
    {
        rc = -errno;
        log_msg(LL_ERROR, "io_setup() failed: %s", strerror(errno));
    }

    return rc;
}

static int
qrtc_file_io_init(struct qrt_config *qrtc)
{
    int oflags = O_DIRECT |
        (qrtVerifyMode ? O_RDONLY : O_TRUNC | O_WRONLY | O_CREAT);

    qrtc->qrtc_fi_info.fi_fd =
        open(qrtc->qrtc_fi_info.fi_output_file_name, oflags, 0644);

    if (qrtc->qrtc_fi_info.fi_fd < 0)
    {
        int rc = errno;
        log_msg(LL_ERROR, "open() failed: %s", strerror(rc));

        return -rc;
    }

    if (qrtVerifyMode)
    {
        struct stat stb;
        int rc = fstat(qrtc->qrtc_fi_info.fi_fd, &stb);
        if (rc != 0)
        {
            int rc = errno;
            log_msg(LL_ERROR, "fstat() failed: %s", strerror(rc));

            return -rc;
        }
        else if (stb.st_size <= 0)
        {
            log_msg(LL_ERROR, "file %s has no size",
                    qrtc->qrtc_fi_info.fi_output_file_name);
            return -EINVAL;
        }

        // take the file size directly from the output file.
        qrtc->qrtc_fi_info.fi_size = stb.st_size;
    }
    else
    {
        // apply the default size if one wasn't given
        if (qrtc->qrtc_fi_info.fi_size <= 0)
            qrtc->qrtc_fi_info.fi_size = DEF_FILE_IO_SIZE;

        if (ftruncate(qrtc->qrtc_fi_info.fi_fd, qrtc->qrtc_fi_info.fi_size))
        {
            int rc = errno;
            log_msg(LL_ERROR, "ftruncate() failed: %s", strerror(rc));

            return -rc;
        }
    }

    return qrtc_aio_init(qrtc);
}

static int
qrtc_rbd_init(struct qrt_config *qrtc)
{
    struct my_rbd_info *rbd_info = qrtc_get_rbd_info(qrtc);

    log_msg(LL_DEBUG, "client-name=%s conf-file=%s image-name=%s",
            qrtc->qrtc_client_name, qrtc->qrtc_conf_file,
            qrtc->qrtc_image_name);

    int rc = rados_create(&rbd_info->rbdi_cluster, qrtc->qrtc_client_name);
    if (rc < 0)
    {
        log_msg(LL_ERROR, "rados_create() failed: %d", rc);
        return rc;
    }

    rc = rados_conf_read_file(rbd_info->rbdi_cluster, qrtc->qrtc_conf_file);
    if (rc < 0)
    {
        log_msg(LL_ERROR, "rados_conf_read_file() failed: %d", rc);
        return rc;
    }

    rc = rados_connect(rbd_info->rbdi_cluster);
    if (rc < 0)
    {
        log_msg(LL_ERROR, "rados_connect() failed: %d", rc);
        return rc;
    }

    rc = rados_ioctx_create(rbd_info->rbdi_cluster, qrtc->qrtc_pool_name,
                            &rbd_info->rbdi_io_ctx);
    if (rc < 0)
    {
        log_msg(LL_ERROR, "rados_ioctx_create() failed: %d", rc);
        qrt_rbd_disconnect(rbd_info);
        return -1;
    }

    rc = rbd_open(rbd_info->rbdi_io_ctx, qrtc->qrtc_image_name,
                  &rbd_info->rbdi_image, NULL /*snap */);
    if (rc < 0)
    {
        log_msg(LL_ERROR, "rbd_open() failed: %d", rc);
        qrt_rbd_disconnect(rbd_info);
        return -1;
    }

    rc = rbd_stat(rbd_info->rbdi_image, &rbd_info->rbdi_stat,
                  sizeof(rbd_info->rbdi_stat));
    if (rc < 0)
    {
        log_msg(LL_ERROR, "rbd_stat() failed: %d", rc);
        qrt_rbd_disconnect(rbd_info);
        return -1;
    }

    if (qrtc_get_num_outputs(qrtc) == 0)
    {
        log_msg(LL_ERROR, "RBD size (%lu) appears too small for this test.",
                rbd_info->rbdi_stat.size);

        return -ENOSPC;
    }

    return 0;
}

static void
qrtc_free_buffers(const struct qrt_config *qrtc)
{
    unsigned i = 0;
    for (i = 0; i < qrtc->qrtc_io_depth; i++)
        if (qrtBufs && qrtBufs[i])
            free(qrtBufs[i]);

    if (qrtBufs)
        free(qrtBufs);
}

static int
qrtc_alloc_buffers(const struct qrt_config *qrtc)
{
    if (qrtBufs)
        return 0;

    qrtBufs = calloc(qrtc->qrtc_io_depth, sizeof(unsigned long long *));
    if (!qrtBufs)
        return -errno;

    unsigned i;
    for (i = 0; i < qrtc->qrtc_io_depth; i++)
    {
        size_t bufsz = qrtc->qrtc_block_size * sizeof(unsigned long long) *
            (qrtc->qrtc_multi_block ? qrtc->qrtc_multi_block : 1);

        int rc = posix_memalign((void *)&qrtBufs[i], 0x1000, bufsz);
        if (rc || !qrtBufs[i])
            return -errno;
    }

    return 0;
}

static void
qrt_src_block_init(const struct qrt_config *qrtc,
                   unsigned long long *block_buf,
                   const size_t block_num, const size_t nblks,
                   const size_t current_overwrite_cnt)
{
    const size_t wpb = qrtc_config_words_per_block(qrtc);
    size_t i;
    for (i = 0; i < nblks; i++)
    {
        unsigned j;
        for (j = 0; j < wpb; j++)
            block_buf[(wpb * i) + j] =
                qrtBlockMagicWord + block_num + i + j + current_overwrite_cnt;
    }
}

static off_t
qrtc_get_block_offset(const struct qrt_config *qrtc, const size_t block_num)
{
    return (off_t)(block_num * qrtc->qrtc_block_size);
}

static int
qrt_sink_block_verify(const struct qrt_config *qrtc,
                      const unsigned long long *block_buf,
                      const size_t block_num,
                      const unsigned short overwrite_val)
{
    ssize_t rem = qrtc->qrtc_block_size;

    while (rem > 0)
    {
        ssize_t rc = qrtc->qrtc_file_io ?
            pread(qrtc->qrtc_fi_info.fi_fd,
                  (((char *)block_buf) + (qrtc->qrtc_block_size - rem)), rem,
                  (qrtc_get_block_offset(qrtc, block_num) +
                   qrtc->qrtc_block_size - rem)) :
            rbd_read(qrtc_get_rbd_info(qrtc)->rbdi_image,
                     (qrtc_get_block_offset(qrtc, block_num) +
                      qrtc->qrtc_block_size - rem), rem,
                     (((char *)block_buf) + (qrtc->qrtc_block_size - rem)));
        if (rc < 0)
        {
            log_msg(LL_ERROR, "rbd_read() error: %zd block-%zu",
                    rc, block_num);
            return -1;
        }

        log_msg(LL_DEBUG, "rbd_read()=%zd %p %p",
                rc, block_buf, (((char *)block_buf) +
                                (qrtc->qrtc_block_size - rem)));

        rem -= rc;
        if (rem < 0)
        {
            log_msg(LL_ERROR, "remaining bytes (%zd) is < 0??", rem);
            return -1;
        }
    }

    unsigned i;
    for (i = 0; i < qrtc_config_words_per_block(qrtc); i++)
    {
        const unsigned long long word_val = block_buf[i];
        if (word_val - (qrtBlockMagicWord + block_num + i + overwrite_val))
        {
            log_msg(LL_ERROR,
                    "b-%08zx word-%u oc-%04hu "
                    "(expected=%012zx, got=%012zx)",
                    block_num, i, overwrite_val,
                    (size_t)(block_num + i + overwrite_val),
                    (size_t)(word_val - qrtBlockMagicWord));
            return -1;
        }

        if (!i)
            log_msg(LL_DEBUG, "OK b-%08zx oc-%04hu (%llx)",
                    block_num, overwrite_val, word_val - qrtBlockMagicWord);
    }
    return 0;
}

/**
 * qrt_block_aio - AIO read/write wrapper which read
 * @qrtc: config info
 * @block_num: block
 */
static int
qrt_block_rbd_aio(const struct qrt_config *qrtc, const size_t block_num,
                  const size_t nblks, const size_t iterations,
                  const size_t iodepth, const bool write_io,
                  const bool overwrite_block)
{
    rbd_completion_t comps[iodepth];

    unsigned i;
    for (i = 0; i < iterations; i++)
    {
        unsigned j;

        // Setup the buffers and completions
        for (j = 0; j < iodepth; j++)
            qrt_src_block_init(qrtc, qrtBufs[j],
                               (overwrite_block ? block_num : block_num + j),
                               nblks, ((i * iodepth) + j));

        for (j = 0; j < iodepth; j++)
        {
            int rc = rbd_aio_create_completion(NULL, NULL, &comps[j]);
            if (rc)
            {
                log_msg(LL_ERROR, "error: rbd_aio_create_completion() %d", rc);
                return rc;
            }

            log_msg(LL_TRACE, "created completion %p %p", comps[j], &comps[j]);
        }

        // Issue I/Os
        for (j = 0; j < iodepth; j++)
        {
            int rc = write_io ?
                rbd_aio_write(qrtc_get_rbd_info(qrtc)->rbdi_image,
                              ((overwrite_block ? block_num : block_num + j) *
                               qrtc->qrtc_block_size),
                              qrtc->qrtc_block_size * nblks,
                              (void *)qrtBufs[j], comps[j]) :
                rbd_aio_read(qrtc_get_rbd_info(qrtc)->rbdi_image,
                             ((overwrite_block ? block_num : block_num + j) *
                               qrtc->qrtc_block_size),
                             qrtc->qrtc_block_size * nblks,
                             (void *)qrtBufs[j], comps[j]);
            if (rc)
            {
                log_msg(LL_ERROR, "error: rbd_aio_%s() %d",
                        write_io ? "write" : "read", rc);
                return rc;
            }

            log_msg(LL_DEBUG,
                    "rbd_aio_%s off=%zx len=%zx rc=%d buf=%p vers=%zu "
                    "word=%llx",
                    write_io ? "write" : "read",
                    ((overwrite_block ? block_num : block_num + j) *
                     qrtc->qrtc_block_size), qrtc->qrtc_block_size * nblks,
                    rc, qrtBufs[j], ((i * iodepth) + j),
                    qrtBufs[j][0] - qrtBlockMagicWord);
        }

        int aio_wait_ret_val = 0;
        ssize_t aio_ret_val = 0;

        for (j = 0; j < iodepth; j++)
        {
            log_msg(LL_TRACE, "wait for completion %u", j);

            ssize_t rrc = 0;
            int rc = rbd_aio_wait_for_complete(comps[j]);
            if (!rc)
                rrc = rbd_aio_get_return_value(comps[j]);

            if (rc ||
                (write_io && rrc) || // write success returns '0' oddly
                (!write_io && rrc != (ssize_t)qrtc->qrtc_block_size))
            {
                log_msg(LL_ERROR, "error: rbd_aio_wait_for_complete()=%d "
                        "rbd_aio_get_return_value()=%zd op=%s",
                        rc, rrc, write_io ? "write" : "read");

                aio_wait_ret_val = rc;
                aio_ret_val = rrc;
            }
        }

        for (j = 0; j < iodepth; j++)
            rbd_aio_release(comps[j]);

        if (aio_wait_ret_val || aio_ret_val)
            return -1;
    }

    return 0;
}

/**
 * qrt_block_aio - AIO read/write wrapper which read
 * @qrtc: config info
 * @block_num: block
 */
static int
qrt_block_file_aio(const struct qrt_config *qrtc, const size_t block_num,
                   const size_t nblks, const size_t iterations,
                   const size_t iodepth, const bool write_io,
                   const bool overwrite_block)
{
    unsigned i;

    for (i = 0; i < iterations; i++)
    {
        unsigned j;

        // Setup the buffers and completions
        for (j = 0; j < iodepth; j++)
            qrt_src_block_init(qrtc, qrtBufs[j],
                               (overwrite_block ? block_num : block_num + j),
                               nblks, ((i * iodepth) + j));

        struct iocb iocb_array[iodepth];
        for (j = 0; j < iodepth; j++)
        {
            size_t count = qrtc->qrtc_block_size * nblks;
            long long offset = ((overwrite_block ? block_num : block_num + j) *
                                qrtc->qrtc_block_size);
            if (write_io)
                io_prep_pwrite(&iocb_array[j], qrtc->qrtc_fi_info.fi_fd,
                               qrtBufs[j], count, offset);
            else
                io_prep_pread(&iocb_array[j], qrtc->qrtc_fi_info.fi_fd,
                              qrtBufs[j], count, offset);
        }

        // explicitly issue the requests one at a time.
        for (j = 0; j < iodepth; j++)
        {
            // it appears that io_submit() doesn't want us to use stack memory
            // for the iocb's.
            struct iocb *iocb = &iocb_array[j];
            int rc = io_submit(qrtAioCtx, 1, &iocb);
            if (rc != 1)
            {
                log_msg(LL_ERROR, "error: io_submit() rc=%d: %s",
                        rc, strerror(errno));
                return rc;
            }

            log_msg(LL_DEBUG,
                    "io_submit() %s off=%llx len=%lx buf=%p vers=%zu "
                    "word=%llx",
                    write_io ? "write" : "read",
                    iocb->u.c.offset, iocb->u.c.nbytes, iocb->u.c.buf,
                    ((i * iodepth) + j), qrtBufs[j][0] - qrtBlockMagicWord);
        }

        struct io_event events[iodepth];
        int aio_wait_ret_val =
            io_getevents(qrtAioCtx, (long)iodepth, (long)iodepth, events,
                         NULL);

        if (aio_wait_ret_val != (long)iodepth)
        {
            log_msg(LL_ERROR, "error: io_getevents() rc=%d: %s",
                    aio_wait_ret_val, strerror(errno));
            return aio_wait_ret_val;
        }

        for (j = 0; j < iodepth; j++)
        {
            if (events[j].res != events[j].obj->u.c.nbytes)
            {
                log_msg(LL_ERROR,
                        "error: event[%d] failed rc=%ld (expected=%ld)",
                        j, events[j].res, events[j].obj->u.c.nbytes);
                return events[j].res;
            }
        }
    }

    return 0;
}

static int
qrt_block_aio(const struct qrt_config *qrtc, const size_t block_num,
              const size_t nblks, const size_t iterations,
              const size_t iodepth, const bool write_io,
              const bool overwrite_block)
{
    return qrtc->qrtc_file_io ?
        qrt_block_file_aio(qrtc, block_num, nblks, iterations, iodepth,
                           write_io, overwrite_block) :
        qrt_block_rbd_aio(qrtc, block_num, nblks, iterations, iodepth,
                          write_io, overwrite_block);
}

/**
 * qrt_assert_output_pos - bounds checking helper.
 */
static inline void
qrt_assert_output_pos(const struct qrt_config *qrtc, const size_t pos)
{
    if (pos >= qrtc_get_num_outputs(qrtc))
        log_msg(LL_FATAL, "invalid pos=%zu is > qrtc_get_num_outputs()=%zu",
                pos, qrtc_get_num_outputs(qrtc));
}

/**
 * qrt_get_output_val - helper which reads a value from the mmap'd output file.
 */
static inline output_val_t
qrt_get_output_val(const struct qrt_config *qrtc, const size_t pos)
{
    qrt_assert_output_pos(qrtc, pos);

    return qrtc->qrtc_output[pos].last_write_val;
}

/**
 * qrt_write_output_val - helper function which stores a value to the mmap'd
 *    output file.  Note, that this function asserts on position and value
 *    bounds.
 */
static inline void
qrt_write_output_val(struct qrt_config *qrtc, const size_t iterations,
                     const size_t iodepth, const size_t pos,
                     const size_t nblks)
{
    size_t i;
    for (i = 0; i < nblks; i++)
    {
        qrt_assert_output_pos(qrtc, pos + i);
        qrt_assert_output_value_bounds(iterations, iodepth);

        qrtc->qrtc_output[pos + i].last_write_val =
            (output_val_t)((iterations * iodepth) - 1);
    }
}


static inline size_t
qrt_get_multi_block_cnt(const struct qrt_config *qrtc)
{
    size_t nblks = 0;
    if (qrtc->qrtc_multi_block > 1)
        nblks = qrt_get_random() % (qrtc->qrtc_multi_block + 1);

    return MAX(nblks, 1);
}

/**
 * qrt_verify_image - Iterates through the mmap'd output file looking for
 *    valid blocks and verifying their contents.
 */
static ssize_t
qrt_verify_image(const struct qrt_config *qrtc)
{
    ssize_t num_errors = 0;
    size_t i;
    for (i = 0; i < qrtc_get_num_outputs(qrtc); i++)
    {
        if (qrt_get_output_val(qrtc, i))
            num_errors +=
                qrt_sink_block_verify(qrtc, qrtBufs[0], i,
                                      qrt_get_output_val(qrtc, i));
        else
            log_msg(LL_TRACE, "block-%zu has no metadata", i);
    }

    return num_errors;
}

#define TIMED_LOOP(runtime)                                               \
    struct timespec _start, _current = {.tv_sec = 0, .tv_nsec = 0};       \
    clock_gettime(CLOCK_MONOTONIC, &_start);                              \
    for (; (ssize_t)((ssize_t)_current.tv_sec - (ssize_t)_start.tv_sec) < \
             runtime;                                                     \
         clock_gettime(CLOCK_MONOTONIC, &_current))

/**
 * qrt_write_image - send many writes and overwrites to random blocks with
 *    random iteration counts and iodepths.
 */
static int
qrt_write_image(struct qrt_config *qrtc)
{
    ssize_t rc = 0;

    TIMED_LOOP(qrtRuntime)
    {
        size_t nblks = qrt_get_multi_block_cnt(qrtc);
        size_t block_num = qrt_get_random() % qrtc_get_num_outputs(qrtc);

        if (block_num + nblks > qrtc_get_num_outputs(qrtc))
            nblks -= (block_num + nblks) - qrtc_get_num_outputs(qrtc);

        size_t iterations = qrt_get_random() % qrtc->qrtc_num_overwrites;
        iterations = MAX(iterations, 1); //prevent '0' iterations

        size_t iodepth = qrt_get_random() % qrtc->qrtc_io_depth;
        iodepth = MAX(iodepth, 1);

        // Lower iodepth to '1' so that QoS can be frequently toggled.
        if (qrt_get_random() % QRT_FORCE_IODEPTH_OF_ONE)
        {
            iterations *= iodepth;
            iodepth = 1;
        }

        rc = qrt_block_aio(qrtc, block_num, nblks, iterations, iodepth, true,
                           true);
        if (rc)
        {
            log_msg(LL_ERROR, "write_block() failed: %zd", rc);
            break;
        }

        qrt_write_output_val(qrtc, iterations, iodepth, block_num, nblks);
    }

    return rc;
}

/**
 * qrt_read_image_for_load_generation_only - thread which reads random blocks
 *    (without integrity checks) for the purpose of generating concurrent
 *    reads alongside the writes.
 */
static void *
qrt_read_image_for_load_generation_only(void *arg)
{
    struct qrt_config *qrtc = arg;
    ssize_t rc;

    // Thread executes in its own pthread context so it requires its own
    // set of private buffers.
    rc = qrtc_alloc_buffers(qrtc);
    if (rc)
        return (void *)rc;

    if (qrtc->qrtc_file_io)
    {
        // allocation an aio context for this thread.
        rc = qrtc_aio_init(qrtc);
        if (rc)
            return (void *)rc;
    }

    TIMED_LOOP(qrtRuntime)
    {
        int iterations = qrt_get_random() % qrtc->qrtc_io_depth;

        while (iterations--)
        {
            // Do a few rounds of read before resting.
            size_t block_num = qrt_get_random() % qrtc_get_num_outputs(qrtc);

            // Don't read past the end of the device.
            unsigned num_aios = MIN((qrt_get_random() % qrtc->qrtc_io_depth),
                                    (qrtc_get_num_outputs(qrtc) - block_num));

            rc = qrt_block_aio(qrtc, block_num, 1, 1, num_aios, false, false);
            if (rc)
            {
                log_msg(LL_ERROR, "qrt_block_aio() failed: %zd", rc);
                break;
            }
        }

        sleep(qrt_get_random() %
              MIN((qrtRuntime / MAX_READ_THREAD_SLEEP_PERCENTAGE_RUNTIME),
                  MAX_READ_THREAD_SLEEP_SECS));
    }

    qrtc_free_buffers(qrtc);

    if (qrtc->qrtc_file_io)
        io_destroy(qrtAioCtx);

    return (void *)rc;
}

static void
qrt_launch_read_threads(struct qrt_config *qrtc)
{
    int i;
    for (i = 0; i < NUM_READ_THREADS; i++)
    {
        if (pthread_create(&qrtReadThreads[i], NULL,
                           qrt_read_image_for_load_generation_only,
                           (void *)qrtc))
            log_msg(LL_FATAL, "pthread_create() failed: %s", strerror(errno));
    }
}

static void
qrt_join_read_threads(void)
{
    int i;
    for (i = 0; i < NUM_READ_THREADS; i++)
    {
        if (pthread_join(qrtReadThreads[i], NULL))
            log_msg(LL_FATAL, "pthread_join() failed: %s", strerror(errno));
    }
}

/**
 * qrt_cleanup - general cleanup handler which disconnects from the rbd volume
 *    and does mmap finalization.
 */
static int
qrt_cleanup(struct qrt_config *qrtc)
{
    if (qrtc->qrtc_file_io)
    {
        io_destroy(qrtAioCtx);

        if (fsync(qrtc->qrtc_fi_info.fi_fd) != 0)
            log_msg(LL_ERROR, "fsync() failed: %s", strerror(errno));

        close(qrtc->qrtc_fi_info.fi_fd);
    }
    else
    {
        qrt_rbd_disconnect(qrtc_get_rbd_info(qrtc));
    }

    int msync_err = 0;
    if (!qrtVerifyMode && qrtc->qrtc_output != NULL)
    {
        msync_err = msync((void *)qrtc->qrtc_output,
                          qrtc_get_output_size(qrtc), MS_SYNC);
        if (msync_err)
            log_msg(LL_ERROR, "msync() failed: %s", strerror(errno));
    }

    int munmap_err = qrtc->qrtc_output != NULL ?
        munmap((void *)qrtc->qrtc_output, qrtc_get_output_size(qrtc)) : 0;

    if (munmap_err)
        log_msg(LL_ERROR, "munmap() failed: %s", strerror(errno));

    int close_err = qrtc->qrtc_output_fd >= 0 ?
        close(qrtc->qrtc_output_fd) : 0;

    if (close_err)
        log_msg(LL_ERROR, "close() failed: %s", strerror(errno));

    if (msync_err)
        return msync_err;

    else if (munmap_err)
        return munmap_err;

    return close_err;
}

int
main(int argc, char **argv)
{
    int cleanup_rc;

    struct qrt_config qrtc;

    qrt_config_init(&qrtc);

    qrt_getopts(argc, argv, &qrtc);

    qrt_config_check(&qrtc);

    int rc = qrtc.qrtc_file_io ?
        qrtc_file_io_init(&qrtc) : qrtc_rbd_init(&qrtc);
    if (rc)
        return rc;

    rc = qrtc_open_and_mmap_output_file(&qrtc);
    if (rc)
    {
        log_msg(LL_ERROR, "open() or mmap() failed: %d", rc);
        goto out;
    }

    if (qrtEnableReadThr && !qrtVerifyMode)
        qrt_launch_read_threads(&qrtc);

    rc = qrtc_alloc_buffers(&qrtc);
    if (rc)
    {
        log_msg(LL_ERROR, "qrtc_alloc_buffers() failed: %d", rc);
        goto out;
    }

    ssize_t errors = qrtVerifyMode ?
        qrt_verify_image(&qrtc) : qrt_write_image(&qrtc);

    if (errors)
        rc = -1;

 out:
    qrtc_free_buffers(&qrtc);

    if (qrtEnableReadThr && !qrtVerifyMode)
        qrt_join_read_threads();

    cleanup_rc = qrt_cleanup(&qrtc);

    return rc ? rc : cleanup_rc;
}
