-- SPDX-FileCopyrightText: 2026 The YAWN authors
-- SPDX-License-Identifier: GPL-2.0-or-later
-- Settle shared-service use at job submission (add to the site's job_submit.lua; needs JobSubmitPlugins=lua).
--
-- (Prefer the site_factor/table plugin: it also refreshes pending jobs. This snippet sets the value once, at submission.)
-- TABLE holds lines "user account delta", written by `settle.py table` (see README.md): what must be added to a job's priority so that it equals the
-- priority the job would have if the user's use of the shared LLM service had been ordinary job usage
-- (delta = PriorityWeightFairshare * (fair-share factor with the service use - without); see settle.py in this directory).
-- Slurm's own usage numbers are not changed; `sprio` shows the value in the SITE column.
-- A missing or unreadable table changes nothing. An admin-set SiteFactor is left alone.

local TABLE = "/path/to/site_factor.txt"   -- written by `settle.py table`
local LIMIT = 10000                         -- = PriorityWeightFairshare

-- lines are "user account delta" (or "user delta"); the line for the job's account wins
local function service_delta(user, account)
   local f = io.open(TABLE, "r")
   if not f then return nil end
   local delta = nil
   for line in f:lines() do
      local u, a, d = line:match("^(%S+)%s+(%S+)%s+(-?%d+)$")
      if not u then u, d = line:match("^(%S+)%s+(-?%d+)$") end
      if u == user then
         if a == nil or account == nil then delta = delta or tonumber(d)
         elseif a == account then delta = tonumber(d) break end
      end
   end
   f:close()
   return delta
end

local function settle(job_desc)
   -- Slurm hands the plugin site_factor = NO_VAL ("not set"), never nil; anything else was set earlier in this script
   if job_desc.site_factor ~= nil and job_desc.site_factor ~= slurm.NO_VAL then return end
   local delta = service_delta(job_desc.user_name, job_desc.account)
   if delta and delta ~= 0 then
      if delta > LIMIT then delta = LIMIT elseif delta < -LIMIT then delta = -LIMIT end   -- never more than the fair-share term itself can move
      job_desc.site_factor = delta
      slurm.log_info("job_submit: service use of %s settled as SiteFactor %d", job_desc.user_name, delta)
   end
end

-- call this from slurm_job_submit(job_desc, part_list, submit_uid), before `return slurm.SUCCESS`.
-- Any error inside is logged and swallowed: the settlement must never be the reason a job is refused.
function settle_service_use(job_desc)
   local ok, err = pcall(settle, job_desc)
   if not ok then slurm.log_error("job_submit: settling service use failed, job left as is: %s", tostring(err)) end
end
