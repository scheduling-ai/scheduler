#set document(title: "Multi-Cluster GPU Scheduling MILP")
#set page(margin: (x: 1.2in, y: 1in), numbering: "1")
#set text(font: "New Computer Modern", size: 10.5pt)
#set par(justify: true, leading: 0.6em)
#set heading(numbering: "1.")
#show heading.where(level: 1): it => {
  v(0.5em)
  block(text(size: 12pt, weight: "bold", it))
  v(0.15em)
}
#show heading.where(level: 2): it => {
  v(0.3em)
  block(text(size: 10.5pt, weight: "bold", it))
  v(0.1em)
}

#align(center)[
  #text(size: 15pt, weight: "bold")[Multi-Cluster GPU Scheduling MILP]
  #v(0.8em)
]

= Problem

Each scheduling cycle determines which GPU workloads to start, keep, suspend, or unsuspend across multiple clusters, and assigns replicas to physical nodes. We formulate this as a mixed-integer linear program and solve it by exact lexicographic optimisation.

= Data

*Pods.* Three disjoint sets: pending $cal(P)$, running $cal(R)$, suspended $cal(S)$. Each pod $j$ has replica count $p_j in bb(Z)_+$, chips per replica $g_j in bb(Z)_+$, total demand $d_j = p_j g_j$, and priority level $r_j$. Vectors $g_cal(P) in bb(Z)_+^(|cal(P)|)$, $g_cal(S) in bb(Z)_+^(|cal(S)|)$ collect the per-replica chip counts.

*Nodes and clusters.* $n$ nodes across clusters $cal(C)$, with capacity vector $overline(c) in bb(Z)_+^n$. Running pod $j in cal(R)$ has a _fixed_ usage vector $u_j in bb(R)_+^n$ (input data, not a variable); $U in bb(R)^(|cal(R)| times n)$ stacks these row-wise. Write $u_j^"run" = bold(1)^top u_j$.

*Candidate indicators.* For pending pod $j$ on cluster $c$, define $a_(j c) in {0,1}^n$ with $(a_(j c))_i = 1$ iff node $i$ has matching chip type and capacity $gt.eq g_j$. Let $cal(F)_j subset.eq cal(C)$ be the feasible clusters for $j$. For suspended pod $j$ (bound to its prior cluster), define $b_j in {0,1}^n$ analogously. These indicators are sparse by construction.

*Quotas.* Pools $cal(Q)$, indexed by (quota, cluster, chip-type) triples, each with guarantee $overline(h)_ell > 0$. Subsets $cal(R)_ell, cal(P)_ell, cal(S)_ell$ partition pods by pool membership.

= Formulation

== Variables

#block(inset: (left: 1em))[
$x in {0,1}^(|cal(P)| times |cal(C)|)$ --- start pending pod $j$ on cluster $c$; #h(0.5em) structurally zero for $c in.not cal(F)_j$ \
$Y in bb(Z)_+^(|cal(P)| times n)$ --- pending replica placement; #h(0.5em) $Y_(j i) lt.eq floor(overline(c)_i \/ g_j)$ \
$k in {0,1}^(|cal(R)|)$ --- keep running ($k_j = 0$ $arrow.r.double$ suspend) \
$w in {0,1}^(|cal(S)|)$ --- unsuspend \
$Z in bb(Z)_+^(|cal(S)| times n)$ --- suspended replica placement; #h(0.5em) $Z_(j i) lt.eq floor(overline(c)_i \/ g_j)$ \
$h in bb(Z)_+^(|cal(Q)|)$ --- quota coverage; #h(0.5em) $h lt.eq.slant overline(h)$ \
]

Running pods carry no placement variables. Their node assignment is fixed input, switched on or off by $k$.

*Schedule indicator.* Define $s_j in {0,1}$ for each pod:
$ s_j = cases(bold(1)^top x_j quad &"if" j in cal(P), k_j &"if" j in cal(R), w_j &"if" j in cal(S).) $

== Constraints

Let $y_j, z_j in bb(Z)_+^n$ denote rows of $Y, Z$ as column vectors. The feasible set $cal(X)$ is:
#v(0.2em)

#block(inset: (left: 1.5em))[
#grid(
  columns: (auto, 1fr, auto),
  column-gutter: 1em,
  row-gutter: 0.7em,
  [(i)], [$x bold(1) lt.eq.slant bold(1)$], [single cluster],
  [(ii)], [$a_(j c)^top y_j = p_j x_(j c), quad forall j in cal(P), space c in cal(F)_j$], [gang scheduling],
  [(iii)], [$b_j^top z_j = p_j w_j, quad forall j in cal(S)$], [gang scheduling],
  [(iv)], [$U^top k + Y^top g_cal(P) + Z^top g_cal(S) lt.eq.slant overline(c)$], [node capacity],
  [(v)], [$h_ell lt.eq.slant "usage"_ell, quad forall ell in cal(Q)$], [quota coverage],
  [(vi)], [$s_j = s_m, quad forall j, m "in same gang set"$], [gang sets],
)
]

#v(0.2em)
where the quota usage for pool $ell = (q, c, a)$ is:
$ "usage"_ell = sum_(j in cal(R)_ell) u_j^"run" k_j + sum_(j in cal(P)_ell) d_j x_(j c) + sum_(j in cal(S)_ell) d_j w_j. $

#v(0.1em)
_Remark._ Constraints (ii)--(iii) imply that quota usage depends only on the binary schedule decisions $(x, k, w)$, not on the integer placements $(Y, Z)$. To see this: by (ii), $g_j (a_(j c)^top y_j) = g_j p_j x_(j c) = d_j x_(j c)$. So the problem decomposes naturally: quotas and priorities operate on _which_ pods run, while capacity constrains _where_ replicas land.

== Lexicographic Objective

Solve sequentially: at stage $i$, maximise $f_i$ over $cal(X)$ subject to $f_t = f_t^star$ for all $t < i$.

#block(inset: (left: 1em))[
#grid(
  columns: (auto, 1fr, auto),
  column-gutter: 1em,
  row-gutter: 0.55em,
  [$f_1$:], [$max bold(1)^top h$], [_quota protection_],
  [$f_2, dots$:], [$max d_r^top s_r, quad r = r_"max", r_("max" - 1), dots$], [_priority (strict lex)_],
  [$f_(dots)$:], [$max (u^"run")^top k$], [_thrash reduction_],
  [$f_(dots)$:], [$max bold(1)^top x bold(1) + bold(1)^top w$], [_pod count_],
  [$f_K$:], [$max beta_cal(P)^top op("vec")(x) + beta_cal(S)^top w$], [_quota alignment_],
)
]

#v(0.1em)
where $d_r$ stacks demands at level $r$, $s_r$ the corresponding schedule indicators, and $beta_cal(P), beta_cal(S)$ are binary vectors indicating whether a quota guarantee exists for each placement. Total MILP solves: $2 + |"priority levels"| + 2$.

= Structural Properties

*Tight formulation.* The gang constraints (ii)--(iii) use the exact coefficient $p_j$ rather than a big-$M$ bound. Combined with the per-variable upper bounds on $Y$ and $Z$, the LP relaxation is tight: fractional solutions are largely precluded by the linking structure. The primary integrality gap comes from the binary schedule decisions, not from fractional replica counts.

*Sparsity.* The indicators $a_(j c)$ and $b_j$ are nonzero only at chip-type-compatible, capacity-sufficient nodes. This reduces the active variable count from $O(|"pods"| dot n)$ to a much smaller set, and the constraint matrices inherit the same sparsity.

*Warm start.* The point $(k, x, w) = (bold(1), 0, 0)$ --- keep all running pods, start nothing, unsuspend nothing --- is always primal feasible. This provides a strong incumbent from the first B&B node.

*Guaranteed feasibility.* Quotas enter only through the objective ($max bold(1)^top h$), not as hard constraints. The feasible set $cal(X)$ is independent of quota targets, so conflicting or oversubscribed quotas degrade gracefully rather than causing infeasibility.

*Lexicographic pruning.* Each equality fix $f_i = f_i^star$ eliminates dominated solutions before the next solve, so later stages face progressively smaller effective problems.

*Limitations.* No partial suspension or live migration. Suspended pods stay bound to their cluster. Pending pods are placed on a single cluster. Single-cycle (no lookahead). No topology awareness beyond chip type.

#pagebreak()

= Worked Example

Two clusters (A, B), four nodes, three pods, one quota pool.

*Input.*

#grid(
  columns: (1fr, 1fr),
  column-gutter: 1em,
  [
    #table(
      columns: (auto, auto, auto, auto),
      stroke: 0.5pt,
      inset: 4pt,
      align: center,
      [*Node*], [*Cluster*], [*Chip*], [$overline(c)_i$],
      [$n_1$], [A], [TPU], [8],
      [$n_2$], [A], [TPU], [8],
      [$n_3$], [B], [GPU], [4],
      [$n_4$], [B], [GPU], [4],
    )
  ],
  [
    #table(
      columns: (auto, auto, auto, auto, auto, auto, auto),
      stroke: 0.5pt,
      inset: 4pt,
      align: center,
      [*Pod*], [*State*], [$p_j$], [$g_j$], [*Chip*], [$r_j$], [*Quota*],
      [$j_1$], [pending], [2], [4], [TPU], [10], [tx],
      [$j_2$], [running, $n_1$], [1], [4], [TPU], [5], [tx],
      [$j_3$], [susp., B], [2], [2], [GPU], [8], [ty],
    )
  ],
)

#v(0.2em)

Quota guarantee: $overline(h)_("tx", A, "TPU") = 8$.  Candidate indicators: $a_(j_1, A) = (1, 1, 0, 0)^top$, $b_(j_3) = (0, 0, 1, 1)^top$.

Data: $U = mat(4, 0, 0, 0)$, #h(0.5em) $g_cal(P) = (4)$, #h(0.5em) $g_cal(S) = (2)$, #h(0.5em) $overline(c) = (8, 8, 4, 4)^top$.

*Optimal solution.* Start $j_1$ on A, keep $j_2$, unsuspend $j_3$:
$ x = mat(1, 0), quad y_(j_1) = (1, 1, 0, 0)^top, quad k = (1), quad w = (1), quad z_(j_3) = (0, 0, 1, 1)^top, quad h = (8). $

*Verification.*

_(i) Single-cluster:_ $x bold(1) = 1 lt.eq.slant 1$. #sym.checkmark

_(ii) Gang (pending):_ $a_(j_1, A)^top y_(j_1) = (1, 1, 0, 0)(1, 1, 0, 0)^top = 2 = p_(j_1) x_(j_1, A) = 2 dot 1$. #sym.checkmark

_(iii) Gang (suspended):_ $b_(j_3)^top z_(j_3) = (0, 0, 1, 1)(0, 0, 1, 1)^top = 2 = p_(j_3) w_(j_3) = 2 dot 1$. #sym.checkmark

_(iv) Capacity:_ $U^top k + Y^top g_cal(P) + Z^top g_cal(S) = (4, 0, 0, 0)^top + (4, 4, 0, 0)^top + (0, 0, 2, 2)^top = (8, 4, 2, 2)^top lt.eq.slant (8, 8, 4, 4)^top$. #sym.checkmark

_(v) Quota:_ $"usage" = u_(j_2)^"run" k_(j_2) + d_(j_1) x_(j_1, A) = 4 dot 1 + 8 dot 1 = 12$, #h(0.5em) so $h = min(12, 8) = 8$. #sym.checkmark

*Lexicographic objective values.*

#table(
  columns: (auto, auto, auto),
  stroke: 0.5pt,
  inset: 4pt,
  align: (left, left, center),
  [*Stage*], [*Objective*], [$f_i^star$],
  [$f_1$: Quota], [$bold(1)^top h = 8$], [$8$],
  [$f_2$: Priority $r = 10$], [$d_(j_1) s_(j_1) = 8 dot 1$], [$8$],
  [$f_3$: Priority $r = 8$], [$d_(j_3) s_(j_3) = 4 dot 1$], [$4$],
  [$f_4$: Priority $r = 5$], [$d_(j_2) s_(j_2) = 4 dot 1$], [$4$],
  [$f_5$: Thrash], [$(u^"run")^top k = 4$], [$4$],
  [$f_6$: Pod count], [$bold(1)^top x bold(1) + bold(1)^top w = 2$], [$2$],
  [$f_7$: Alignment], [$beta_(j_1, A) + 0 = 1$], [$1$],
)

#v(0.3em)
_Remark._ After fixing $f_1^star = 8$ (quota) and $f_2^star = 8$ (top priority), the solver must start $j_1$. Thrash maximisation ($f_5$) then forces $k_(j_2) = 1$ over suspension. Pod count ($f_6$) activates $j_3$. The lexicographic stages progressively narrow the feasible set until a unique solution remains.
