Hi Aaron-
   Here's a spreadsheet that tells you what % of each Mfg X Specialty group is attributable to OP spending over a 2 year time span. 
   This gives the aggregates for each combo - if you sort by attributable $, the NPs and PAs really pop out, just huge amounts of dollars they are spending. I think this could really be a focus - and it kind of fits with the story. 

   Here's the overview of the methodology if you want to be able to defend it: 

1) We take every doctor and calculate for each month:
How much they prescribe in rx$ for each company that month
How much OP $ they take from each company that month 
2) Then we calculate for each NPI x Month x Manufacturer: 
What was my average prescription $ over the last 6 months for this manufacturer  ? 
How much OP$ did I take this month? 
What was my average prescription $ over the next 6 months for this manufacturer
3) Then we run a regression to estimate how much OP$ this month influences rx$ over the next 6 months, controlling for my specialty, year/month, and how much i prescribed before. So this is basically saying - if I take $X in OP, how much will by rx$ go up (by what %)? 

4) Then we use that to estimate if the doctor took $0 in OP, how much would we think they prescribe? Given that they took the observed amount of OP (maybe 0, maybe not) how much do we think they would prescribe? Then we look and say "oh, without OP we think they'd prescribe 100k, but with the OP, we think it'll be 110k, so 10% of their rx$ is due to OP"

5) Then we aggregate and sum and average and do all the basic analytics 101 stuff. 

-David