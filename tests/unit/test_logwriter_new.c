#include "lib/logwriter.h"
#include "lib/logqueue-fifo.h"
#include "lib/apphook.h"
#include "lib/logproto/logproto-text-client.h"

#define msg_string "kakukk"

MsgFormatOptions parse_options;

static LogMessage * 
givenAnyLogMessage()
{
  GSockAddr *sa;
  LogMessage *msg;

  sa = g_sockaddr_inet_new("10.10.10.10", 1010);
  msg = log_msg_new(msg_string, strlen(msg_string), sa, &parse_options);
  g_sockaddr_unref(sa);
  return msg;
};

static gssize
log_transport_fake_write(LogTransport *s, const gpointer buf, gsize buflen)
{
  return -1;
};

LogTransport *log_transport_fake_new()
{
  LogTransport *transport = g_new0(LogTransport, 1);
  log_transport_init_method(transport, 0);
  transport->write = log_transport_fake_write;
};

void test_log_writer_should_not_leak_when_message_is_not_consumed()
{
  GlobalConfig *cfg = cfg_new(0x0);
  LogWriter *log_writer = log_writer_new(0);
  LogQueue *queue = log_queue_fifo_new(100, "alma");
  LogWriterOptions options;
  LogPathOptions path_options = LOG_PATH_OPTIONS_INIT;
  LogMessage *msg;
  int i;

  plugin_load_module("syslogformat", cfg, NULL);
  msg_format_options_defaults(&parse_options);
  msg_format_options_init(&parse_options, cfg);

  for (i = 0; i < 100; i++)
    {
      msg = givenAnyLogMessage();
      log_queue_push_tail(queue, msg, &path_options);
    }

  LogProtoTextClient *text_client = log_proto_text_client_new(log_transport_fake_new(), &options.proto_options.super);

  log_writer_options_defaults(&options);
  log_writer_options_init(&options, cfg, 0);

  log_writer_set_options(log_writer, NULL, &options, 0, 0, NULL, NULL);
  log_writer_set_queue(log_writer, queue);
  log_pipe_init(log_writer, cfg);
  log_writer_reopen(log_writer, text_client);
  
  log_pipe_deinit(log_writer);
  log_pipe_unref(log_writer);

  cfg_free(cfg);
};

int main()
{
  app_startup();
  test_log_writer_should_not_leak_when_message_is_not_consumed();
  app_shutdown();
}
