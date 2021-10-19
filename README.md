# Lab 2 Report

> This repository is left empty on purpose. You can copy your solution from
> Lab 1 into this repository, and continue to work from there.
> Do not copy over the README.md, as the report for Lab 2 will have a different
> template from Lab 1.

> This is the template for your lab 2 report. Some guidelines and rules apply.

> You can only change the *contents* of the sections, but not the section 
> headers above the second level of hierarchy! This means you can only add
> headers starting with `###`. 
 
> You must remove the text fields prefixed with `>`, such as this one, resulting
> in a README.md without this template text in between.

> Any report violating these rules will be immediately rejected for corrections 
> (i.e. as if there is no report). 

## Usage

> Describe how to use your program. You can assume the TAs who will be grading
> this know how to do everything that is in the lab manual. You do not have to
> repeat how to use e.g. Docker etc.

> Please do explain how to provide the correct inputs to your program.

## Approach

> Describe significant iterations (see Rubric) of your implementation.
>
> Note down relevant metrics such as: which dataset did you run on what system,
> how much memory did this use, what was the run-time, cost, throughput, etc..
> or whatever is applicable in your story towards your final implementation.
>
> You can show both application-level improvements and cluster-level 
> improvements here.

> Example:
> ### Iteration 0: Baseline
> After achieving robustness level 3, we ultimately performed the following
> experiment on the baseline implementation
>
> | | |
> |---|---|
> | System                  | Laptop X |
> | Workers                 | 4 | 
> | Dataset                 | Netherlands | 
> | Run time <br>(hh:mm:ss) | 00:15:04 | 
> | (more relevant metrics) | (more relevant values) |
>  
> ### Iteration 1: Improving X
> By inspecting the application profile of the previous iteration, we discovered
> that (some clearly referenced step in the code named X) caused a bottleneck,
> taking up 90% of the total run time.
> 
> We came up with three alternatives:
> * Alternative A: to replace the step with (explanation of some approach)
> * Alternative B: to replace the step with (explanation of another approach)
> 
> We have ultimately chosen alternative B, because (discussion of weighing the
> alternatives and reasoning leading to choice of alternative B).
> 
> After implementing this alternative, we re-ran the experiment, resulting in:
>  
> | | |
> |---|---|
> | System                  | Laptop X |
> | Workers                 | 4 | 
> | Dataset                 | Netherlands | 
> | Run time <br>(hh:mm:ss) | 00:10:04 | 
> | (more relevant metrics) | (more relevant values) |
> 
> ### Iteration 2: Improving Y
> Since the previous iteration didn't bring us to the target run-time of ...

## Summary of application-level improvements

> A point-wise summary of all application-level improvements (very short, about
> one or two 80 character lines per improvement, just to provide an overview).
 
## Cluster-level improvements

> A point-wise summary of all cluster-level improvements (very short, about
> one or two 80 character lines per improvement, just to provide an overview).

## Conclusion

> Briefly conclude what you have learned.
