dofile("table_show.lua")
dofile("urlcode.lua")
local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
JSON = (loadfile "JSON.lua")()

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local item_type = nil
local item_name = nil
local item_value = nil

local item_domain = nil
local item_domain_enjin_name = nil
local item_preset = nil
local item_thread = nil

if urlparse == nil or http == nil then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false

local discovered_outlinks = {}
local discovered_items = {}
local bad_items = {}
local ids = {}

local post_requests = {}
local last_id = 1

local retry_url = false

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end

abort_item = function(item)
  abortgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
    if string.match(item, "boxsmall") then
      discover_item(target, string.gsub(item, "boxsmall", "boxlarge"))
    end
--print('discovered', item)
    target[item] = true
    return true
  end
  return false
end

find_path_loop = function(url, max_repetitions)
  local tested = {}
  local tempurl = urlparse.unescape(url)
  tempurl = string.match(tempurl, "^https?://[^/]+(.*)$")
  if not tempurl then
    return false
  end
  for s in string.gmatch(tempurl, "([^/%?&]+)") do
    s = string.lower(s)
    if not tested[s] then
      if s == "" then
        tested[s] = -2
      else
        tested[s] = 0
      end
    end
    tested[s] = tested[s] + 1
    if tested[s] == max_repetitions then
      return true
    end
  end
  return false
end

find_item = function(url)
  local value = string.match(url, "^https?://www%.enjin%.com/page/([0-9]+)$")
  local type_ = "site_id"
  local preset = nil
  local thread = nil
  if not value then
    value = string.match(url, "^https?://www%.enjin%.com/profile/([0-9]+)$")
    type_ = "profile"
  end
  if not value and not string.match(url, "/page/[0-9]") then
    value, preset, thread = string.match(url, "^https?://([^/]+.*)/m/([0-9]+)/viewthread/([0-9]+)")
    type_ = "thread"
    if thread == item_thread then
      value = nil
    end
  end
  if not value and not string.match(url, "/page/[0-9]") then
    value, preset, thread = string.match(url, "^https?://([^/]+.*)/m/([0-9]+)/viewforum/([0-9]+)")
    type_ = "forum"
    if thread == item_thread then
      value = nil
    end
  end
  if value then
    item_type = type_
    if type_ == "thread" or type_ == "forum" then
      item_domain = value
      item_preset = preset
      item_thread = thread
      item_value = item_domain .. ":" .. item_preset .. ":" .. item_thread
    else
      item_domain = nil
      item_preset = nil
      item_thread = nil
      item_value = value
    end
    item_name_new = item_type .. ":" .. item_value
    if item_name_new ~= item_name then
      ids = {}
      ids[value] = true
      item_domain_enjin_name = nil
      abortgrab = false
      initial_allowed = false
      tries = 0
      item_name = item_name_new
      print("Archiving item " .. item_name)
    end
  end
end

is_static = function(url)
  return string.match(url, "files%.enjin%.com")
    or string.match(url, "^https?://resources%.enjin%.com")
    or string.match(url, "^https?://assets%-cloud%.enjin%.com")
    or string.match(url, "^https?://files%-cloud%.enjin%.com")
    or string.match(url, "^https?://[^/]+/assets/")
    or string.match(url, "^https?://[^/]+/fonts/")
    or string.match(url, "^https?://[^/]+/admin/")
    or string.match(url, "^https?://[^/]+/themes/")
end

allowed = function(url, parenturl)
  if ids[url] then
    return true
  end

  if find_path_loop(url, 10) then
    return false
  end

  if (
      string.match(url, "^https?://[^/]+/profile/")
      and item_type ~= "profile"
    )
    or string.match(url, "^https?://[^/]+.*/m/.+/most%-views")
    or string.match(url, "^https?://[^/]+.*/m/.+/most%-replies")
    or string.match(url, "^https?://[^/]+.*/m/.+/post/[0-9]+")
    or string.match(url, "/ajax%.php.+thread_id=[0-9]")
    or string.match(url, "/ajax%.php.+forum%-thread")
    or string.match(url, "/ajax%.php.+comment_id=[0-9]")
    or string.match(url, "^https?://https?://")
    or string.match(url, "^https?://www%.facebook%.com/") then
    return false
  end

  if item_type ~= "profile"
    and string.match(url, "^https?://assets%-cloud%.enjin%.com/users/[^/]+/avatar/") then
    return false
  end

  if item_type ~= "profile" and not string.match(url, "/page/[0-9]") then
    local a, b, name, c = string.match(url, "^https?://([^/]+.*)/m/([0-9]+)/view([a-z]+)/([0-9]+)")
    if not a then
      a, name, c, b = string.match(url, "^https?://([^/]+.*)/view([a-z]+)/([0-9]+)/m/([0-9]+)")
    end
    if (name == "forum" or name == "thread") and a and b and c and c ~= item_thread then
      discover_item(discovered_items, name .. ":" .. a .. ":" .. b .. ":" .. c)
      return false
    end
  end

  if item_type == "site_id" then
    for s in string.gmatch(url, "([0-9]+)") do
      if s == item_value then
        return true
      end
    end
    if item_domain then
      local s = string.match(url .. "/", "^https?://([^%./]+)%.enjin%.com/")
      if (s and string.lower(s) == string.lower(item_domain_enjin_name))
        or (not s and string.match(string.lower(url) .. "/", "^https?://[^/]-([^%./]+%.[a-z]+)/") == string.match(string.lower(item_domain), "([^%.]+%.[a-z]+)$")) then
        return true
      end
    end
  end

  if item_type == "thread" or item_type == "forum" then
    for s in string.gmatch(url, "([0-9]+)") do
      if s == item_thread then
        return true
      end
    end
  end

  if is_static(url)
    or (
      not string.match(url, "^https?://[^/]*enjin%.com/")
      and (
        not item_domain
        or string.match(string.lower(url) .. "/", "^https?://[^/]-([^%./]+%.[a-z]+)/") ~= string.match(string.lower(item_domain), "([^%.]+%.[a-z]+)$")
      )
    ) then
    if string.match(url, "^https?://[^%./]+%.[^/]+") then
      discover_item(discovered_outlinks, url)
    end
    return false
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  --[[if allowed(url, parent["url"]) and not processed(url) then
    addedtolist[url] = true
    return true
  end]]

  --[[if html == 0 then
    discover_item(discovered_outlinks, url)
  end]]

  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  
  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function decode_codepoint(newurl)
    newurl = string.gsub(
      newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
      function (s)
        return unicode_codepoint_as_utf8(tonumber(s, 16))
      end
    )
    return newurl
  end

  local function check(newurl)
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "%s") then
      for s in string.gmatch(newurl, "([^%s]+)") do
        check(s)
      end
      return nil
    end
    local origurl = url
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_)
      and allowed(url_, origurl) then
--print('queued', url_)
      table.insert(urls, { url=url_ })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function json_post_request(url, action, data, base_url)
    local request_id = action .. " " .. tostring(data["title"])
    if not addedtolist[request_id] then
      addedtolist[request_id] = true
      last_id = last_id + 1
      post_requests[last_id] = {
        ["jsonrpc"]="2.0",
        ["id"]=last_id,
        ["method"]=action,
        ["params"]=data
      }
      --print(last_id, action)
      local newurl = urlparse.absolute(url, "/api/v1/api.php")
      ids[newurl] = true
      table.insert(urls, {
        url=newurl,
        method="POST",
        body_data=JSON:encode(post_requests[last_id])
      })
      post_requests[last_id]["extra"] = {
        ["base_url"]=base_url
      }
    end
  end

  if string.match(url, "viewforum")
    or string.match(url, "viewthread") then
    local page_s = string.match(url, "(/page/[0-9]+)")
    if page_s then
      local newurl = string.gsub(url, page_s, "")
      newurl = string.match(newurl, "^(.-)/*$") .. page_s
      check(newurl)
      for i=1,tonumber(string.match(page_s, "([0-9]+)$")) do
        check(string.gsub(url, page_s, "/page/" .. tostring(i)))
      end
    end
  end

  if string.match(url, "/article/.*/page/[0-9]+$") then
    check(string.match(url, "^(.+)/page/[0-9]+$"))
  end

  if allowed(url)
    and status_code < 300
    and not is_static(url) then
    html = read_file(file)
    if string.match(html, "var album_url")
      and not string.match(html, "total_albums:%s*0") then
      kill_grab()
      return urls
    end
    local wiki_id, preset_id, base_url = string.match(html, "m_wiki%[([0-9]+)%]%s*=%s*new%s+Enjin_Wiki%({%s*preset_id%s*:%s*([0-9]+),%s*base_url%s*:%s+'([^']+)',")
    if wiki_id and preset_id and base_url then
      local preset_id_data = {
        ["preset_id"]=preset_id
      }
      check(urlparse.absolute(url, base_url .. "all-pages"))
      check(urlparse.absolute(url, base_url .. "empty-pages"))
      check(urlparse.absolute(url, base_url .. "no-category"))
      check(urlparse.absolute(url, base_url .. "category-thumbs"))
      check(urlparse.absolute(url, base_url .. "category-list"))
      check(urlparse.absolute(url, base_url .. "recent-changes"))
      json_post_request(url, "Wiki.getPageList", preset_id_data, base_url)
      json_post_request(url, "Wiki.getEmptyPages", preset_id_data, base_url)
      json_post_request(url, "Wiki.getNoCategoryPages", preset_id_data, base_url)
      json_post_request(url, "Wiki.getCategories", preset_id_data, base_url)
      json_post_request(url, "Wiki.getFiles", preset_id_data, base_url)
      json_post_request(url, "Wiki.getRecentChanges", preset_id_data, base_url)
      json_post_request(url, "Wiki.getStats", preset_id_data, base_url)
    end
    if string.match(url, "/api/v1/api%.php") then
      local json = JSON:decode(html)
      if json["transport"] == "POST" then
        return urls
      end
      local request_json = post_requests[tonumber(json["id"])]
      local request_method = request_json["method"]
      local base_url = request_json["extra"]["base_url"]
      local preset_id = request_json["params"]["preset_id"]
      if request_method == "Wiki.getPageList"
        or request_method == "Wiki.getEmptyPages"
        or request_method == "Wiki.getNoCategoryPages"
        or request_method == "Wiki.getCategories" then
        for _, data in pairs(json["result"]) do
          check(urlparse.absolute(url, base_url .. "page/" .. data["page_title"]))
          check(urlparse.absolute(url, base_url .. "page-history/" .. data["page_title"]))
          check(urlparse.absolute(url, base_url .. "page-editors/" .. data["page_title"]))
          check(urlparse.absolute(url, base_url .. "page-comments/" .. data["page_title"]))
          json_post_request(url, "Wiki.getPageTitle", {
            ["preset_id"]=preset_id,
            ["title"]=data["page_title"],
            ["prop"]={"text", "categories", "likes", "comments"}
          }, base_url)
          json_post_request(url, "Wiki.getPageHistory", {
            ["preset_id"]=preset_id,
            ["title"]=data["page_title"],
            ["from"]=0,
            ["to"]=1700000000
          }, base_url)
          json_post_request(url, "Wiki.getPageHistory", {
            ["preset_id"]=preset_id,
            ["title"]=data["page_title"]
          }, base_url)
          json_post_request(url, "Wiki.getPageCommentData", {
            ["preset_id"]=preset_id,
            ["title"]=data["page_title"]
          }, base_url)
        end
      elseif request_method == "Wiki.getPageTitle" then
        if json["result"]["text_display"] then
          html = html .. "\n\n" .. json["result"]["text_display"]
        end
        if json["result"]["text_text"] then
          html = html .. "\n\n" .. json["result"]["text_text"]
        end
      end
    end
    html = string.gsub(html, "\\", "")
    html = string.gsub(html, '<select%s+style="float:right"%s+class="input%-text%s+input%-select%-thin"%s+onchange="document%.location%.href=this%.options%[this%.selectedIndex%]%.value;">[^<]+.-</select>[^<]+</div>', "")
    if item_type == "thread" then
      check("https://www.eso-rp.com/ajax.php?s=redirect&cmd=forum-unreadpost&preset=" .. item_preset .. "&thread_id=" .. item_thread)
      check("https://www.enjin.com/ajax.php?s=redirect&cmd=forum-thread&id=" .. item_thread .. "&preset=" .. item_preset)
    end
    if item_type == "site_id" and item_domain == nil then
      local site = string.match(html, 'class=\'site%-url\'%s*href="([^"]+)">')
      if site then
        item_domain = string.lower(string.match(site, "^https?://([^/]+)"))
        item_domain_enjin_name = string.lower(string.match(site, "^https?://([^%./]+)%.enjin%.com"))
      end
      check(site)
      if not string.match(site, "/$") then
        site = site .. "/"
      end
      check(site .. "sitemap")
    end
    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
    html = string.gsub(html, "&gt;", ">")
    html = string.gsub(html, "&lt;", "<")
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  find_item(url["url"])
  if not item_name then
    error("No item name found.")
  end
  if status_code ~= 200
    and string.match(url["url"], "^https?://www%.enjin%.com/page/([0-9]+)$") then
    io.stdout:write("Does not exist.\n")
    io.stdout:flush()
    abort_item()
  end
  if status_code ~= 200
    and status_code ~= 404
    and status_code ~= 301
    and status_code ~= 302
    and not is_static(url["url"]) then
    io.stdout:write("Server returned bad response. Skipping.\n")
    io.stdout:flush()
    abort_item()
  end
  if abortgrab then
    return false
  end
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]
  
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()

  if killgrab then
    return wget.actions.ABORT
  end

  if not is_static(url["url"]) then
    os.execute("sleep " .. tostring(concurrency*1.5))
  end

  find_item(url["url"])

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end
  
  if status_code < 400 then
    downloaded[url["url"]] = true
  end

  if status_code == 0 or abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 10
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and JSON:decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["enjin-4nai036j4bmxmjly"] = discovered_items,
    ["urls-r7ut587q8z4cwe16"] = discovered_outlinks
  }) do
    print('queuing for', string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 100 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end

