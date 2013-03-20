#include "aflua-source.h"
#include "logsource.h"
#include "mainloop.h"
#include "aflua-dest.h"

typedef struct _LuaReader
{
   LogSource super;
   lua_State* state;
   struct iv_task reader_start;
   GThread *thread;
   GMutex *thread_mutex;
} LuaReader;

void aflua_sd_queue(LogPipe* pipe, LogMessage *msg, LogPathOptions *path_options, gpointer user_data)
{
   log_pipe_forward_msg(pipe, msg, path_options);
   return;
}

static int lua_post(lua_State* state)
{
   LuaReader* self = lua_topointer(state, 1);
   LogPathOptions path_options = LOG_PATH_OPTIONS_INIT;
   const char* msg = g_strdup(lua_tostring(state, 2));
   LogMessage* lm = log_msg_new_internal(0, msg);
   log_msg_refcache_start_producer(lm);
   log_pipe_queue(&self->super.super, lm, &path_options);
   log_msg_refcache_stop();
   return 0;
}


static void lua_reader_thread(void* data)
{ 
   
   LogPathOptions path_options = LOG_PATH_OPTIONS_INIT;
   LuaReader* self = (LuaReader*) data;
   /*
   while(1)
   {
     LogMessage* lm = log_msg_new_internal(0, "kakukk");
     log_msg_refcache_start_producer(lm);
     log_pipe_queue(&self->super.super, lm, &path_options);
     log_msg_refcache_stop();
     sleep(10);
   }*/
   lua_getglobal(self->state, "thread_func");
   lua_pushlightuserdata(self->state, self);
   lua_pcall(self->state, 1, 0, 0);
}

static void lua_start_thread(LuaReader* self)
{
  msg_debug("Starting LuaReader thread", NULL);
  self->thread_mutex = g_mutex_new();
  self->thread = create_worker_thread(lua_reader_thread, self, TRUE, NULL);
  //main_loop_external_thread_started();
  //TODO: implemented external thread started
}

gboolean lua_reader_init(LogPipe* pipe)
{
   LuaReader* self = (LuaReader*) pipe;
   msg_debug("Initializing LuaReader", NULL);
   iv_task_register(&self->reader_start);
   return TRUE;
}

gboolean lua_reader_deinit(LogPipe* pipe)
{
  LuaReader* self = (LuaReader*) pipe;
  lua_close(self->state);
  return TRUE;
}

void lua_reader_free(LogPipe* self)
{
}



LogPipe* lua_reader_new(GString* func, LogSourceOptions* options)
{
   LuaReader* self = g_new0(LuaReader, 1);
   log_source_init_instance(&self->super);
   log_source_set_options(&self->super, options, 0, SCS_INTERNAL, NULL, NULL, FALSE);
   self->super.super.init = lua_reader_init;
   self->super.super.deinit = lua_reader_deinit;
   self->super.super.free_fn = lua_reader_free;
   IV_TASK_INIT(&self->reader_start);
   self->reader_start.cookie = self;
   self->reader_start.handler = lua_start_thread;
   self->state = lua_open();
   luaL_openlibs(self->state);
   lua_register(self->state, "post", lua_post);
   aflua_load_bytecode_into_state(self->state, "thread_func", func);
   return self;
}

gboolean aflua_sd_deinit(LogPipe* s)
{
   AFLuaSourceDriver* self = (AFLuaSourceDriver*) s;
   log_pipe_deinit(self->reader);
}

void aflua_sd_free(LogPipe* s)
{
  
}

gboolean aflua_sd_init(LogPipe* s)
{
   AFLuaSourceDriver* self = (AFLuaSourceDriver*) s;
   GlobalConfig *cfg = log_pipe_get_config(&self->super.super.super);
   log_source_options_init(&self->source_options, cfg, self->super.super.group);
   log_source_options_defaults(&self->source_options);
   self->reader = lua_reader_new(self->thread_func, &self->source_options);
   log_pipe_append(self->reader, &self->super.super.super);
   log_pipe_init(self->reader, cfg);
}


LogDriver* aflua_sd_new(GString* function)
{
   msg_debug("Creating Lua Source driver", NULL);
   AFLuaSourceDriver* self = g_new0(AFLuaSourceDriver,1);
   self->thread_func = function;
   log_src_driver_init_instance(&self->super);
   self->super.super.super.init = aflua_sd_init;
   self->super.super.super.deinit = aflua_sd_deinit;
   self->super.super.super.queue = aflua_sd_queue;
   self->super.super.super.free_fn = aflua_sd_free;
   return &self->super.super;
}

void aflua_source_driver(lua_State* state)
{
   GString* byte_code;
   byte_code = aflua_get_bytecode_from_parameter(state, 1);
   LogDriver* d = aflua_sd_new(byte_code);
   lua_create_userdata_from_pointer(state, d, LUA_SOURCE_DRIVER_TYPE);
   return 1;
}

void aflua_register_lua_source(lua_State* state)
{
  lua_register(state, "LuaSource", aflua_source_driver);
}

