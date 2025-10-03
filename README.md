# nexus-cache

A distributed key-value cache with gossip-based replication for eventual consistency build on HashiCorp's memberlist.

## Why this exists

Why not?

## What it does

A lightweight cache that uses gossip protocol to replicate entries across nodes. Each node maintains a full copy of the cache, making reads always local and fast. I just needed this for a project I'm working on. :)



