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

person_a places two orders with different addresses (o_delivered, o_open), like the 252 people
whose address changes. These rows are test inputs only and never reach a lake or a metric.
