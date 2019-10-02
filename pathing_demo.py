#!/usr/bin/env python
# coding: utf-8

# # Pathing Demo notebook

# ## Setup

# In[1]:


# get_ipython().run_line_magic('matplotlib', 'inline')
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from textwrap import wrap

from pathutils import (get_popular_urls, funnel_in_outs,funnel_stats, sankey_funnel, 
                       frequent_funnel, analyze_clicks, analyze_traffic, utils, manage_resolutions, 
                       url_regex_resolver, analyze_timing)


# In[2]:


#HAUSERDIR = "<Path to your Hauser folder>"
HAUSERDIR = "sampledata"

# `LIMITROWS` limits the number of rows in the output of most popular URLs
LIMITROWS = 0


# ## Load Hauser data into a dataframe

# In[3]:


dffull = analyze_traffic.get_hauser_as_df(HAUSERDIR, navigate_only=False)
dffull = utils.preproc_traffic(dffull)
#Optional: you can also filter your dataset to only include sessions with clicks of certain type
#dffull = analyze_clicks.filter_dataset_by_clicktype(dffull, "rage")
df = analyze_clicks.remove_non_navigation(dffull)
useResolvedUrls = False


# ## Inspect your dataframe(s)

# In[8]:


dffull.head(5)


# In[9]:


df.head(5)


# ## Plot a diagram of top most visited URLs

# In[4]:


url_counts = get_popular_urls.get_popular(df, useResolvedUrls, LIMITROWS)
TOPCOUNTS = 20 # limit output rows
analyze_traffic.plot_counts_by_freq(url_counts, TOPCOUNTS, "URL Counts", False)


# ## Show common funnels that include the specified URL

# In[5]:


TESTURL = "https://www.oodatime.com/cart"
FUNNELLEN = 3
NUMFUNNELSTOSHOW = 4
top_funnels = frequent_funnel.get_top_funnels_df(TESTURL, FUNNELLEN, useResolvedUrls, df, LIMITROWS)
frequent_funnel.print_top_funnel_counts(top_funnels, NUMFUNNELSTOSHOW)


# ## Show conversion statistics for the specified funnel

# In[6]:


test_funnel=["https://www.oodatime.com/collections/mens",
             "https://www.oodatime.com/collections/mens/products/blue-watch",
             "https://www.oodatime.com/cart"]
funnel_counts = funnel_stats.get_funnel_stats(df, test_funnel, useResolvedUrls, LIMITROWS)
analyze_traffic.plot_counts_by_freq(funnel_counts, 0, "Funnel Counts", True)


# ## Plot sankey diagram for the specified funnel

# In[8]:


sankey_funnel.plot_funnel("Blue Watch Funnel", df, test_funnel, useResolvedUrls, cutoff=4)


# ## Generate session links for the specified funnel

# **Note:** This only works if you are a FullStory user (use your Org ID)

# In[8]:


ORGID = "NHQ5G"
STAGING = False
sessions = analyze_traffic.get_sessions_for_funnel(df, test_funnel, useResolvedUrls, ORGID, STAGING, True, 5)
for s in sessions:
    print(s)


# ## Generate session links for a funnel that include a specified click type

# In[9]:


ORGID = "NHQ5G"
STAGING = False
clicktype = "rage"
sessions = analyze_traffic.get_sessions_for_funnel_and_click(dffull, test_funnel, clicktype, useResolvedUrls, ORGID, STAGING, True, 5)
for s in sessions:
    print(s)


# ## Print timing stats for a funnel

# In[10]:


funtimes = analyze_timing.get_timing_for_funnel(dffull, test_funnel, useResolvedUrls)
analyze_timing.print_timing_averages(test_funnel, funtimes)


# ## Generate timing histogram for 1 step of the funnel

# In[11]:


analyze_timing.plot_timing_data(test_funnel, funtimes, 0)


# ## Generate timing histogram for all steps of the funnel

# In[12]:


analyze_timing.plot_timing_data(test_funnel, funtimes, -1)


# ## Generate inflow and outflow counts for the specified funnel

# In[13]:


ingress, egress = funnel_in_outs.get_in_outs(df, test_funnel, useResolvedUrls, LIMITROWS)


# ## Plot inflow statistics

# In[14]:


analyze_traffic.plot_counts_by_freq(ingress, 0, "Ingress", False)


# ## Plot outflow statistics

# In[15]:


analyze_traffic.plot_counts_by_freq(egress, 0, "Egress", False)


# ## Plot most visited URLs again (for illustration purposes)

# In[16]:


url_counts = get_popular_urls.get_popular(df, useResolvedUrls, LIMITROWS)
TOPCOUNTS = 20
analyze_traffic.plot_counts_by_freq(url_counts, TOPCOUNTS, "URL Counts", False)


# ## Display current list of URL resolution rules

# In[17]:


manage_resolutions.show_rules()


# ## Add URL resolution rule

# In[18]:


manage_resolutions.add_rule("/products/(black|blue|red|gold|rainbow)-watch","/products/<any-watch>")


# ## Display URL resolution rules again

# In[19]:


manage_resolutions.show_rules()


# ## Plot most visited resolved URLs

# In[20]:


useResolvedUrls = True
url_counts = get_popular_urls.get_popular(df, useResolvedUrls, LIMITROWS)
TOPCOUNTS = 20
analyze_traffic.plot_counts_by_freq(url_counts, TOPCOUNTS, "URL Counts", False)


# In[ ]:




