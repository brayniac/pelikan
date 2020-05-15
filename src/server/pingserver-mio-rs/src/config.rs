pub struct Config {
	daemonize: bool,
	pid_filename: Option<String>,
	dlog_intvl: usize,

	admin: AdminConfig,
	server: ServerConfig,
	debug: DebugConfig,
}

pub struct AdminConfig {
	host: Option<String>,
	port: String,
	timeout: usize,
	nevent: usize,
	tw_tick: usize,
	tw_cap: usize,
	tw_ntick: usize,
}

impl Default for AdminConfig {
	fn default() -> Self {
		Self {
			host: None,
			port: "9999".to_string(),
			timeout: 100,
			nevent: 1024,
			tw_tick: 10,
			tw_cap: 1000,
			tw_ntick: 100,
		}
	}
}

impl Default for Config {
	fn default() -> Self {
		Self {
			daemonize: false,
			pid_filename: None,
			dlog_intvl: 500,

			admin: AdminConfig::default(),
			server: ServerConfig::default(),
		}
	}
}

pub struct DebugConfig {
	log_level: usize,
	log_file: Option<String>,
	lob_nbuf: usize,
}

impl Default for DebugConfig {
	fn default() -> Self {
		Self {
			log_level: 4,
			log_file: None,
			log_nbuf: 0,
		}
	}
} 

pub struct WorkerConfig {
	timeout: usize,
	nevent: usize,
}

impl Default for WorkerConfig {
	fn default() -> Self {
		Self {
			timeout: 100,
			nevent: 1024,
		}
	}
}

pub struct ServerConfig {
	host: Option<String>,
	port: String,
	timeout: usize,
	nevent: usize,
}

impl Default for ServerConfig {
	fn default() -> Self {
		host: None,
		port: "12321",
		timeout: 100,
		nevent: 1024,
	}
}

/*
#define PINGSERVER_OPTION(ACTION)                                                        \
    ACTION( daemonize,      OPTION_TYPE_BOOL,   false,  "daemonize the process"        )\
    ACTION( pid_filename,   OPTION_TYPE_STR,    NULL,   "file storing the pid"         )\
    ACTION( dlog_intvl,     OPTION_TYPE_UINT,   500,    "debug log flush interval(ms)" )

#define ADMIN_HOST      NULL
#define ADMIN_PORT      "9999"
#define ADMIN_TIMEOUT   100     /* in ms */
#define ADMIN_NEVENT    1024
#define ADMIN_TW_TICK   10      /* in ms */
#define ADMIN_TW_CAP    1000    /* 1000 ticks in timing wheel */
#define ADMIN_TW_NTICK  100     /* 1 second's worth of timeout events */

/*          name            type                default         description */
#define ADMIN_OPTION(ACTION)                                                                    \
    ACTION( admin_host,     OPTION_TYPE_STR,    ADMIN_HOST,     "admin interfaces listening on")\
    ACTION( admin_port,     OPTION_TYPE_STR,    ADMIN_PORT,     "admin port"                   )\
    ACTION( admin_timeout,  OPTION_TYPE_UINT,   ADMIN_TIMEOUT,  "evwait timeout"               )\
    ACTION( admin_nevent,   OPTION_TYPE_UINT,   ADMIN_NEVENT,   "evwait max nevent returned"   )\
    ACTION( admin_tw_tick,  OPTION_TYPE_UINT,   ADMIN_TW_TICK,  "timing wheel tick size (ms)"  )\
    ACTION( admin_tw_cap,   OPTION_TYPE_UINT,   ADMIN_TW_CAP,   "# ticks in timing wheel"      )\
    ACTION( admin_tw_ntick, OPTION_TYPE_UINT,   ADMIN_TW_NTICK, "max # ticks processed at once")

#define SERVER_HOST     NULL
#define SERVER_PORT     "12321"
#define SERVER_TIMEOUT  100     /* in ms */
#define SERVER_NEVENT   1024

#define SERVER_OPTION(ACTION)                                                                   \
    ACTION( server_host,    OPTION_TYPE_STR,    SERVER_HOST,    "interfaces listening on"      )\
    ACTION( server_port,    OPTION_TYPE_STR,    SERVER_PORT,    "port listening on"            )\
    ACTION( server_timeout, OPTION_TYPE_UINT,   SERVER_TIMEOUT, "evwait timeout"               )\
    ACTION( server_nevent,  OPTION_TYPE_UINT,   SERVER_NEVENT,  "evwait max nevent returned"   )

typedef struct {
    SERVER_OPTION(OPTION_DECLARE)
} server_options_st;

/*          name                    type            description */
#define CORE_SERVER_METRIC(ACTION)                                                   \
    ACTION( server_event_total,     METRIC_COUNTER, "# server events returned"      )\
    ACTION( server_event_loop,      METRIC_COUNTER, "# server event loops returned" )\
    ACTION( server_event_read,      METRIC_COUNTER, "# server core_read events"     )\
    ACTION( server_event_write,     METRIC_COUNTER, "# server core_write events"    )\
    ACTION( server_event_error,     METRIC_COUNTER, "# server core_error events"    )

#define WORKER_TIMEOUT   100     /* in ms */
#define WORKER_NEVENT    1024

/*          name            type                default         description */
#define WORKER_OPTION(ACTION)                                                                   \
    ACTION( worker_timeout, OPTION_TYPE_UINT,   WORKER_TIMEOUT, "evwait timeout"               )\
    ACTION( worker_nevent,  OPTION_TYPE_UINT,   WORKER_NEVENT,  "evwait max nevent returned"   )

typedef struct {
    WORKER_OPTION(OPTION_DECLARE)
} worker_options_st;

/*          name                    type            description */
#define CORE_WORKER_METRIC(ACTION)                                                   \
    ACTION( worker_event_total,     METRIC_COUNTER, "# worker events returned"      )\
    ACTION( worker_event_loop,      METRIC_COUNTER, "# worker event loops returned" )\
    ACTION( worker_event_read,      METRIC_COUNTER, "# worker core_read events"     )\
    ACTION( worker_event_write,     METRIC_COUNTER, "# worker core_write events"    )\
    ACTION( worker_event_error,     METRIC_COUNTER, "# worker core_error events"    )\
    ACTION( worker_add_stream,      METRIC_COUNTER, "# worker adding a stream"      )\
    ACTION( worker_ret_stream,      METRIC_COUNTER, "# worker returning a stream"   )

#define DEBUG_LOG_LEVEL 4       /* default log level */
#define DEBUG_LOG_FILE  NULL    /* default log file */
#define DEBUG_LOG_NBUF  0       /* default log buf size */

/*          name             type              default           description */
#define DEBUG_OPTION(ACTION)                                                            \
    ACTION( debug_log_level, OPTION_TYPE_UINT, DEBUG_LOG_LEVEL,  "debug log level"     )\
    ACTION( debug_log_file,  OPTION_TYPE_STR,  DEBUG_LOG_FILE,   "debug log file"      )\
    ACTION( debug_log_nbuf,  OPTION_TYPE_UINT, DEBUG_LOG_NBUF,   "debug log buf size"  )

enum {
    TIME_UNIX     = 0,
    TIME_DELTA    = 1,
    TIME_MEMCACHE = 2,
    TIME_SENTINEL = 3
};

/*          name          type                default       description */
#define TIME_OPTION(ACTION) \
    ACTION( time_type,    OPTION_TYPE_UINT,   TIME_UNIX,    "Expiry timestamp mode" )

#define NELEM_DELTA 16


/*          name                type                default           description */
#define ARRAY_OPTION(ACTION)                                                                              \
    ACTION( array_nelem_delta,  OPTION_TYPE_UINT,   NELEM_DELTA,      "max nelem delta during expansion" )

*/