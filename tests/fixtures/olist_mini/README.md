# olist_mini

Hand-built test input shaped like the Olist files, so CI can run without the real data (which is
never committed). Each order covers one case the real data contains:

| order | case |
| --- | --- |
| o_delivered | normal lifecycle; two items; two payments listed out of sequence order; two reviews |
| o_undated | status delivered but no delivery date (8 real orders) |
| o_never_approved | canceled, never approved, but has a payment row (141 real orders) |
| o_unavailable | unavailable after approval |
| o_open | still shipped when the data was extracted (open) |
| o_cancel_after_delivery | canceled although it has a delivery date (6 real orders) |
| o_2016 | bought on 28 Dec 2016, delivered and reviewed in January 2017: the truth slice (orders from 2017-01-01) must drop all of its events |

person_a places two orders with different addresses (o_delivered, o_open), like the 252 people
whose address changes. Two sellers and three products are included: p_2 has no category and no
dimensions (610 and 2 real products), and p_3's category, pc_gamer, has no English translation.
These rows are test inputs only and never reach a lake or a metric.
