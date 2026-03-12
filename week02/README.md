<p class="p1">Assignment 2: Migrate Distributed Prime Service from HTTP/JSON to Schema-Based gRPC</p>
<p class="p2"><b>Context</b></p>
<p class="p3">Given a working distributed prime-number system from week 1 with:</p>
<ul class="ul1">
  <li class="li3"><span class="s1">primes_in_range.py</span> (core prime logic; <b>keep this algorithm</b>)</li>
  <li class="li4">secondary_node.py<span class="s2"> (HTTP worker with </span>/compute<span class="s2">, </span>/health<span class="s2">, </span>/info<span class="s2">)</span></li>
  <li class="li4">primary_node.py<span class="s2"> (HTTP coordinator with </span>/register<span class="s2">, </span>/nodes<span class="s2">, </span>/compute<span class="s2">)</span></li>
  <li class="li3"><span class="s1">primes_cli.py</span> (CLI with local + distributed execution paths)</li>
</ul>
<p class="p3">Your job is to <b>rewrite the network/control plane to gRPC + Protocol Buffers</b> (schema-first RPC), while preserving behavior.</p>
<p class="p5"><br></p>
<p class="p6"><b>Learning Objectives</b></p>
<p class="p3">By completing this assignment, you will be able to:</p>
<ul class="ul1">
  <li class="li3">Design a protobuf schema for distributed RPC workflows.</li>
  <li class="li3">Replace ad-hoc HTTP/JSON APIs with strongly-typed gRPC services.</li>
  <li class="li3">Implement coordinator-worker fanout with deadlines, errors, and aggregation.</li>
  <li class="li3">Keep business logic stable while changing transport + serialization.</li>
  <li class="li3">Validate correctness and compare performance against the original design.</li>
</ul>
<p class="p5"><br></p>
<p class="p6"><b>What You Must Build</b></p>
<p class="p2"><b>1) Protobuf schema (</b><span class="s1"><b>proto/primes.proto</b></span><b>)</b></p>
<p class="p3">Define typed messages and services for:</p>
<ul class="ul1">
  <li class="li3"><b>Coordinator service</b> (formerly <span class="s1">primary_node</span> endpoints):</li>
  <ul class="ul1">
    <li class="li4">RegisterNode</li>
    <li class="li4">ListNodes</li>
    <li class="li4">Compute</li>
  </ul>
  <li class="li3"><b>Worker service</b> (formerly <span class="s1">secondary_node</span> endpoints):</li>
  <ul class="ul1">
    <li class="li4">ComputeRange</li>
    <li class="li4">Health</li>
  </ul>
</ul>
<p class="p3">Use enums (not strings) for execution and mode:</p>
<ul class="ul1">
  <li class="li4">Mode<span class="s2">: </span>COUNT<span class="s2">, </span>LIST</li>
  <li class="li4">ExecMode<span class="s2">: </span>SINGLE<span class="s2">, </span>THREADS<span class="s2">, </span>PROCESSES</li>
</ul>
<p class="p5"><br></p>
<p class="p2"><b>2) gRPC servers</b></p>
<ul class="ul1">
  <li class="li3"><span class="s1">primary_node.py</span> becomes a gRPC server implementing <span class="s1">CoordinatorService</span>.</li>
  <li class="li3"><span class="s1">secondary_node.py</span> becomes a gRPC server implementing <span class="s1">WorkerService</span>.</li>
  <li class="li3">Secondary should register itself to primary on startup using gRPC.</li>
  <li class="li3">Primary must keep in-memory node registry + TTL expiration behavior.</li>
</ul>
<p class="p5"><br></p>
<p class="p2"><b>3) gRPC client path in CLI</b></p>
<p class="p4"><span class="s2">Update </span>primes_cli.py<span class="s2">:</span></p>
<ul class="ul1">
  <li class="li3">Keep local modes (<span class="s1">single</span>, <span class="s1">threads</span>, <span class="s1">processes</span>) as-is.</li>
  <li class="li3">Replace distributed HTTP call with gRPC call to coordinator <span class="s1">Compute</span>.</li>
  <li class="li3">Preserve existing CLI flags and output format as closely as possible.</li>
</ul>
<p class="p5"><br></p>
<p class="p2"><b>4) Preserve existing behavior</b></p>
<p class="p3">Keep semantics from the current system:</p>
<ul class="ul1">
  <li class="li4"><span class="s2">range </span>[low, high)</li>
  <li class="li4">count<span class="s2"> and </span>list<span class="s2"> modes</span></li>
  <li class="li3">chunking and worker options</li>
  <li class="li3">per-node summary option</li>
  <li class="li3">max-return-primes cap/truncation behavior</li>
  <li class="li3">graceful errors (invalid range, no nodes, bad mode, etc.)</li>
</ul>
<p class="p5"><br></p>
<p class="p6"><b>Group Implementation Instructions (Task-by-Task)</b></p>
<p class="p2"><b>Step 0 — Setup</b></p>
<ul class="ul1">
  <li class="li1">Group leader should start this project by copy the content from week01 directory into a new directory called week02 and commit it to the main branch. <span class="Apple-converted-space"> </span></li>
  <li class="li1">All group members will create their branches from the main branch that has the week02 directory.<span class="Apple-converted-space">  </span>All your work for this project must be done in the week02 folder.<span class="Apple-converted-space">  </span>Group members should pick 1 or more tasks to work on. <span class="Apple-converted-space"> </span></li>
  <li class="li1">Each team member should work on their own branches and then merge your code into the main branch (by a pull request) when your work is complete.<span class="Apple-converted-space">  </span>It is a good idea to keep updating your branch incrementally (having many small commits) instead of performing a single commit.</li>
  <li class="li1">Make sure you can use the workstation in the Linux lab.<span class="Apple-converted-space">  </span>If you had issues before, please submit the details in the survey that was sent last week and email me with screenshots if possible.</li>
  <li class="li1">Remember, you will be tested on your code.</li>
</ul>
<p class="p3">Install dependencies:</p>
<p class="p7">python -m pip install grpcio grpcio-tools protobuf</p>
<p class="p2"><b>Task 1 — Create schema first</b></p>
<p class="p3">Create <span class="s1">proto/primes.proto</span> and define:</p>
<ul class="ul1">
  <li class="li3">Messages for node metadata, compute requests/responses, health, list nodes.</li>
  <li class="li3"><span class="s1">Compute</span> request fields equivalent to existing distributed payload:</li>
  <ul class="ul1">
    <li class="li4">low<span class="s2">, </span>high<span class="s2">, </span>mode<span class="s2">, </span>chunk<span class="s2">, </span>secondary_exec<span class="s2">, </span>secondary_workers<span class="s2">,</span></li>
    <li class="li4">max_return_primes<span class="s2">, </span>include_per_node</li>
  </ul>
</ul>
<p class="p3">Generate code:</p>
<p class="p7">python -m grpc_tools.protoc \</p>
<p class="p7"><span class="Apple-converted-space">  </span>-I./proto \</p>
<p class="p7"><span class="Apple-converted-space">  </span>--python_out=. \</p>
<p class="p7"><span class="Apple-converted-space">  </span>--grpc_python_out=. \</p>
<p class="p7"><span class="Apple-converted-space">  </span>./proto/primes.proto</p>
<p class="p2"><b>Task 2 — Implement WorkerService</b></p>
<p class="p4"><span class="s2">In </span>secondary_node.py<span class="s2">:</span></p>
<ul class="ul1">
  <li class="li3">Implement <span class="s1">ComputeRange</span> by reusing existing partition + compute logic.</li>
  <li class="li3">Implement <span class="s1">Health</span>.</li>
  <li class="li3">On startup, create coordinator stub and call <span class="s1">RegisterNode</span>.</li>
  <li class="li3">Return typed protobuf responses instead of JSON dicts.</li>
</ul>
<p class="p2"><b>Task 3 — Implement CoordinatorService</b></p>
<p class="p4"><span class="s2">In </span>primary_node.py<span class="s2">:</span></p>
<ul class="ul1">
  <li class="li4"><span class="s2">Implement </span>RegisterNode<span class="s2">, </span>ListNodes<span class="s2">, </span>Compute<span class="s2">.</span></li>
  <li class="li3">In <span class="s1">Compute</span>, split range into node slices and fan out <span class="s1">ComputeRange</span> calls to workers.</li>
  <li class="li3">Aggregate results exactly as before.</li>
  <li class="li3">Enforce timeouts/deadlines on downstream worker RPCs.</li>
  <li class="li3">Convert downstream RPC errors to structured gRPC status/details.</li>
</ul>
<p class="p2"><b>Task 4 — Update CLI distributed mode</b></p>
<p class="p4"><span class="s2">In </span>primes_cli.py<span class="s2">:</span></p>
<ul class="ul1">
  <li class="li3">Replace <span class="s1">_post_json(primary/compute)</span> with coordinator stub call.</li>
  <li class="li3">Preserve console output behavior for count/list/time/per-node reporting.</li>
</ul>
<p class="p2"><b>Task 5 — Add tests</b></p>
<p class="p3">Create tests for:</p>
<ul class="ul1">
  <li class="li3"><span class="s1">count</span> correctness for known ranges.</li>
  <li class="li3"><span class="s1">list</span> correctness + truncation flag.</li>
  <li class="li3">no active workers error.</li>
  <li class="li3">bad input (<span class="s1">high &lt;= low</span>) error mapping.</li>
  <li class="li3">one integration test with 1 primary + 2 workers.</li>
</ul>
<p class="p2"><b>Task 6 — Run demo in class</b></p>
<p class="p3">Expected demo flow:</p>
<ul class="ul1">
  <li class="li3">Start primary gRPC server.</li>
  <li class="li3">Start two secondary workers; each registers with primary.</li>
  <li class="li3">Run CLI distributed count and list.</li>
  <li class="li3">Show per-node summary and elapsed timings.</li>
  <li class="li3">Compare the performance between HTTP/JSON approach in week 1, and the gRPC approach in week 2.</li>
</ul>
<p class="p8"><br></p>
<p class="p1">Grading Rubric (100 points)</p>
<ul class="ul1">
  <li class="li3"><b>Schema design quality (20)</b>: clear enums/messages, no stringly-typed RPC.</li>
  <li class="li3"><b>Correctness (25)</b>: outputs match original behavior.</li>
  <li class="li3"><b>gRPC implementation (20)</b>: coordinator + worker + registration flow.</li>
  <li class="li3"><b>Resilience (15)</b>: deadlines, error mapping, invalid input handling.</li>
  <li class="li3"><b>Testing &amp; reproducibility (10)</b>: automated or scripted validation.</li>
  <li class="li3"><b>Documentation (10)</b>: clear setup/run/migration explanation.</li>
</ul>
<p class="p5"><br></p>
