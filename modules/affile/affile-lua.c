#include "affile-dest.h"
#include "affile-source.h"
#include <lua.h>
#include "luaconfig.h"

int affile_lua_dd(lua_State* state)
{
   const char* name = g_strdup(lua_tostring(state, 1));
   LogDriver *s = affile_dd_new(name, 0);
   lua_create_userdata_from_pointer(state, s, LUA_DESTINATION_DRIVER_TYPE);
   return 1;
}

int affile_lua_sd(lua_State* state)
{
   const char* name = g_strdup(lua_tostring(state, 1));
   LogDriver *s = affile_sd_new(name, 0);
   lua_parse_reader_options(state, &(((AFFileSourceDriver*)s)->reader_options));
   lua_create_userdata_from_pointer(state, s, LUA_SOURCE_DRIVER_TYPE);
   return 1;
}

void affile_register_lua_config(GlobalConfig* cfg)
{
   lua_State* state = cfg->lua_cfg_state;
   msg_debug("Registering affile lua config items", NULL);
   lua_register(state, "FileSource", affile_lua_sd);
   lua_register(state, "FileDestination", affile_lua_dd);
}
