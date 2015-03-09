# clusterWild

To compile / package, run
<code>&lt;sbt&gt; package</code>, where &lt;sbt&gt; is your local sbt compiler.

To excute, follow the instructions at <a href="https://spark.apache.org/docs/1.2.0/submitting-applications.html">this page</a>, specifically (in your spark directory):
<pre><code class="bash">./bin/spark-submit <span class="se">\</span>
  --class &lt;main-class&gt;
  --master &lt;master-url&gt; <span class="se">\</span>
  --deploy-mode &lt;deploy-mode&gt; <span class="se">\</span>
  --conf &lt;key&gt;<span class="o">=</span>&lt;value&gt; <span class="se">\</span>
  ... <span class="c"># other options</span>
  &lt;application-jar&gt; <span class="se">\</span>
  <span class="o">[</span>application-arguments<span class="o">]</span>
</code></pre>

For example,
<code>./bin/spark-submit --class "ClusterWildV03Dimitris" --master local[2] &lt;clusterWild dir&gt;/target/scala-2.10/clusterwild_2.10-1.0.jar</code>
where &lt;clusterWild dir&gt; is your clusterWild directory.
