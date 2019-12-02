#!/usr/bin/env python
# coding: utf-8

# # Pathing Demo notebook

# ## Setup

# In[ ]:


# get_ipython().run_line_magic('matplotlib', 'inline')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from textwrap import wrap

from pathutils import (get_popular_urls, funnel_in_outs,funnel_stats, sankey_funnel, 
                       frequent_funnel, analyze_clicks, analyze_traffic, utils, manage_resolutions, 
                       url_regex_resolver, analyze_timing)


# In[ ]:


#HAUSERDIR = "<Path to your Hauser folder>"
HAUSERDIR = "sampledata"

# `LIMITROWS` limits the number of rows in the output of most popular URLs
LIMITROWS = 0


# Hauser is an open source tool you can use to export data from FullStory into your filesystem: https://github.com/fullstorydev/hauser

# ## Load Data Export data into a dataframe

# In[ ]:


dffull = analyze_traffic.get_hauser_as_df(HAUSERDIR, navigate_only=False)


# ## Inspect your dataframe(s)
# You can find Data Export field descriptions on FullStory's API reference site: https://developer.fullstory.com/get-data-export

# In[ ]:


dffull = utils.preproc_events(dffull)
dffull.head(15)


# ## Filter out any events that aren't navigation events

# In[ ]:


#Optional: you can also filter your dataset to only include sessions with clicks of certain type
#dffull = analyze_clicks.filter_dataset_by_clicktype(dffull, "rage")
df = analyze_clicks.remove_non_navigation(dffull)
df.head(15)


# ## Plot a diagram of top most visited URLs

# In[ ]:


useResolvedUrls = False
url_counts = get_popular_urls.get_popular(df, useResolvedUrls, LIMITROWS)
TOPCOUNTS = 20 # limit output rows
analyze_traffic.plot_counts_by_freq(url_counts, TOPCOUNTS, "URL Counts", False)


# ## Show common funnels that include the specified URL

# In[ ]:


TESTURL = "https://www.oodatime.com/cart"
FUNNELLEN = 3
NUMFUNNELSTOSHOW = 4
top_funnels = frequent_funnel.get_top_funnels_df(TESTURL, FUNNELLEN, useResolvedUrls, df, LIMITROWS)
frequent_funnel.print_top_funnel_counts(top_funnels, NUMFUNNELSTOSHOW)


# ## Show conversion statistics for the specified funnel

# In[ ]:


test_funnel=["https://www.oodatime.com/collections/mens",
             "https://www.oodatime.com/collections/mens/products/blue-watch",
             "https://www.oodatime.com/cart"]
funnel_counts = funnel_stats.get_funnel_stats(df, test_funnel, useResolvedUrls, LIMITROWS)
analyze_traffic.plot_counts_by_freq(funnel_counts, 0, "Funnel Counts", True)


# ## Plot sankey diagram for the specified funnel

# In[ ]:


sankey_funnel.plot_funnel("Blue Watch Funnel", df, test_funnel, useResolvedUrls, cutoff=4)


# ## Generate session links for the specified funnel

# **Note:** This only works if you are a FullStory user (use your Org ID)

# In[ ]:


ORGID = "NHQ5G"
STAGING = False
sessions = analyze_traffic.get_sessions_for_funnel(df, test_funnel, useResolvedUrls, ORGID, STAGING, True, 5)
for s in sessions:
    print(s)


# ## Generate session links for a funnel that include a specified click type

# In[ ]:


ORGID = "NHQ5G"
STAGING = False
clicktype = "rage"
sessions = analyze_traffic.get_sessions_for_funnel_and_click(dffull, test_funnel, clicktype, useResolvedUrls, ORGID, STAGING, True, 5)
for s in sessions:
    print(s)


# ## Print timing stats for a funnel

# In[12]:


funtimes = analyze_timing.get_timing_for_funnel(dffull, test_funnel, useResolvedUrls)
analyze_timing.print_timing_averages(test_funnel, funtimes)


# ## Generate timing histogram for 1 step of the funnel

# In[13]:


analyze_timing.plot_timing_data(test_funnel, funtimes, 0)


# ## Generate timing histogram for all steps of the funnel

# In[14]:


analyze_timing.plot_timing_data(test_funnel, funtimes, -1)


# ## Generate inflow and outflow counts for the specified funnel

# In[15]:


ingress, egress = funnel_in_outs.get_in_outs(df, test_funnel, useResolvedUrls, LIMITROWS)


# ## Plot inflow statistics

# In[16]:


analyze_traffic.plot_counts_by_freq(ingress, 0, "Ingress", False)


# ## Plot outflow statistics

# In[17]:


analyze_traffic.plot_counts_by_freq(egress, 0, "Egress", False)


# ## Plot most visited URLs again (for illustration purposes)

# In[18]:


url_counts = get_popular_urls.get_popular(df, useResolvedUrls, LIMITROWS)
TOPCOUNTS = 20
analyze_traffic.plot_counts_by_freq(url_counts, TOPCOUNTS, "URL Counts", False)


# ## Display current list of URL resolution rules

# In[19]:


manage_resolutions.show_rules()


# ## Add URL resolution rule

# In[20]:


manage_resolutions.add_rule("/products/(black|blue|red|gold|rainbow)-watch","/products/<any-watch>")


# ## Display URL resolution rules again

# In[21]:


manage_resolutions.show_rules()


# ## Plot most visited resolved URLs

# In[22]:


useResolvedUrls = True
url_counts = get_popular_urls.get_popular(df, useResolvedUrls, LIMITROWS)
TOPCOUNTS = 20
analyze_traffic.plot_counts_by_freq(url_counts, TOPCOUNTS, "URL Counts", False)

