# TPC-DS Toolkit

The official TPC-DS tools can be found at [tpc.org](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp).

TPC-DS version: 3.2.0

## Setup

Make sure the required development tools are installed.

Ubuntu:

```bsah
sudo apt-get install gcc make flex bison byacc git
```

## Dataset Generation

Make sure at `tools` folder, then use `make` to compile the data generation tool

If there is an error of multiple definition, such as
```
multiple definition of `nItemIndex'; 
multiple definition of `g_s_web_order_lineitem'
multiple definition of `g_w_catalog_page'
multiple definition of `g_w_warehouse'; 
multiple definition of `g_w_web_site'; 
```

Then, it is better to switch to gcc-9
```
make CC=/usr/bin/gcc-9
```
or switch to gcc-9 using `update-alternatives`

## Queries Running

Fixable Query Issues:

> Ambiguous reference to column name "item_id" (use: "ws_items.item_id" or "ss_items.item_id") in Query 58 
> Fix: Change to `ss_items.item_id` in order by predicate

> Ambiguous reference to column name "d_week_seq" (use: "d1.d_week_seq" or "d3.d_week_seq") in Query 72
> Fix: Change to `d1.d_week_seq` in order by predicate

> syntax error at or near "returns" in Query 77


