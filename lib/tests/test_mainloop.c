#include <libtest/testutils.h>
#include "cfg.h"
#include "logpipe.h"
#include "messages.h"
#include "cfg-tree.h"
#include "driver.h"

/* the old configuration that is being reloaded */
extern GlobalConfig *main_loop_old_config;
/* the pending configuration we wish to switch to */
extern GlobalConfig *main_loop_new_config;

typedef struct _MockPipe {
  LogSrcDriver super;
  gint state;
} MockPipe;

gboolean
log_pipe_test_source_init(LogPipe *s) 
{
  MockPipe *self = (MockPipe *)s;
  static gint run = 0;
  fprintf(stderr, "kakukk\n");
  GlobalConfig *cfg = log_pipe_get_config(&self->super.super.super);
  self->state = GPOINTER_TO_INT(cfg_persist_config_fetch(cfg, "alma"));
  if (self->state == 0)
    self->state = 1;
  else
    self->state++;
  fprintf(stderr, "alma: %d\n", self->state);
  run++;
  assert_gint(self->state, run, "Run number and state does not equal");
  return TRUE;
};

gboolean
log_pipe_test_source_deinit(LogPipe *s) 
{
  MockPipe *self = (MockPipe *)s;
  fprintf(stderr, "deinit kakukk\n");
  GlobalConfig *cfg = log_pipe_get_config(&self->super.super.super);
  cfg_persist_config_add(cfg, "alma", GINT_TO_POINTER(self->state), NULL, FALSE); 
  return TRUE;
};

gboolean 
log_pipe_test_dest_init(LogPipe *self)
{
  fprintf(stderr, "dest kakukk\n");
  return TRUE;
};

gboolean
log_pipe_test_dest_fail_init(LogPipe *self)
{
  fprintf(stderr, "dest fail kakukk\n");
  return FALSE;
};

LogPipe* create_source(GlobalConfig *cfg)
{
  LogPipe *pipe = g_new0(MockPipe, 1);
  log_src_driver_init_instance(pipe, cfg);
  pipe->init = log_pipe_test_source_init;
  pipe->deinit = log_pipe_test_source_deinit;
  return pipe;
};

LogPipe* create_good_dest(GlobalConfig *cfg)
{
  LogPipe *dpipe = g_new0(LogPipe, 1);
  log_pipe_init_instance(dpipe, cfg);
  dpipe->init = log_pipe_test_dest_init;
  return dpipe;
};

LogPipe* create_bad_dest(GlobalConfig *cfg)
{
  LogPipe *dpipe = g_new0(LogPipe, 1);
  log_pipe_init_instance(dpipe, cfg);
  dpipe->init = log_pipe_test_dest_fail_init;
  return dpipe;
};

void init_config(GlobalConfig *cfg, LogPipe *spipe, LogPipe *dpipe)
{
  LogExprNode *source = log_expr_node_new_source(NULL, log_expr_node_new_pipe(spipe, NULL), NULL);
  LogExprNode *dest = log_expr_node_new_destination(NULL, log_expr_node_new_pipe(dpipe, NULL), NULL);
  
  cfg_tree_add_object(&cfg->tree, source);
  cfg_tree_add_object(&cfg->tree, dest);

  return cfg;
};

GlobalConfig* create_and_init_old_configuration()
{
  GlobalConfig *cfg = cfg_new(0x0);
  LogPipe *spipe = create_source(cfg);
  LogPipe *dpipe = create_good_dest(cfg);

  init_config(cfg, spipe, dpipe);

  main_loop_initialize_state(cfg, "lofasz");

  return cfg;
};

GlobalConfig* create_new_configuration()
{

  GlobalConfig *cfg = cfg_new(0x0);
  LogPipe *spipe = create_source(cfg);
  LogPipe *dpipe = create_bad_dest(cfg);

  init_config(cfg, spipe, dpipe);
  return cfg;
};

int main()
{

  app_startup();

  log_stderr = TRUE;
  debug_flag = TRUE;

  main_loop_old_config = create_and_init_old_configuration();
  main_loop_new_config = create_new_configuration();

  main_loop_reload_config_apply();

  app_shutdown();

  return 0;

};
