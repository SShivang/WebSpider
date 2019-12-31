package ir.webutils;

import java.util.*;
import java.io.*;

import ir.utilities.*;

import java.net.*;

/**
 * Spider defines a framework for writing a web crawler.  Users can
 * change the behavior of the spider by overriding methods.
 * Default spider does a breadth first crawl starting from a
 * given URL up to a specified maximum number of pages, saving (caching)
 * the pages in a given directory.  Also adds a "BASE" HTML command to
 * cached pages so links can be followed from the cached version.
 *
 * @author Ted Wild and Ray Mooney
 */

public class PageRankSpider extends Spider {

  /**
   * Checks command line arguments and performs the crawl.  <p> This
   * implementation calls <code>processArgs</code> and
   * <code>doCrawl</code>.
   *
   * @param args Command line arguments.
   */

   // graph of the web
   Graph webGraph;

   // store the page ranks
   HashMap<String, Double>  pageRanks;

   // store the page count
   int pageCount;

   // store the downloaded pages
   HashMap<String, String> urlToIndex;


  public void go (String[] args){
    processArgs(args);
    doCrawl();
    // perform page rank with 0.15 as alpha
    pageRank(0.15);
  }

  /*
    Initialize the page rank spider constructor.
  */
  public PageRankSpider(){
    webGraph = new Graph();
    pageRanks = new HashMap<String, Double>();
    urlToIndex = new HashMap<String, String>();
    pageCount = 0;
  }


  /**
   * Performs the crawl.  Should be called after
   * <code>processArgs</code> has been called.  Assumes that
   * starting url has been set.  <p> This implementation iterates
   * through a list of links to visit.  For each link a check is
   * performed using {@link #visited visited} to make sure the link
   * has not already been visited.  If it has not, the link is added
   * to <code>visited</code>, and the page is retrieved.  If access
   * to the page has been disallowed by a robots.txt file or a
   * robots META tag, or if there is some other problem retrieving
   * the page, then the page is skipped.  If the page is downloaded
   * successfully {@link #indexPage indexPage} and {@link
   * #getNewLinks getNewLinks} are called if allowed.
   * <code>go</code> terminates when there are no more links to visit
   * or <code>count &gt;= maxCount</code>
   */
  public void doCrawl() {

    if (linksToVisit.size() == 0) {
      System.err.println("Exiting: No pages to visit.");
      System.exit(0);
    }
    visited = new HashSet<Link>();

    HashMap<String, ArrayList<String>> nodes = new HashMap<String, ArrayList<String>>();

    while (linksToVisit.size() > 0 && count < maxCount) {

      if (slow) {
        synchronized (this) {
          try {
            wait(1000);
          }
          catch (InterruptedException e) {
          }
        }
      }

      // Take the top link off the queue
      Link link = linksToVisit.remove(0);
      link.cleanURL(); // Standardize and clean the URL for the link
      System.out.println("Trying: " + link);
      // Skip if already visited this page
      if (!visited.add(link)) {
        System.out.println("Already visited");
        continue;
      }
      if (!linkToHTMLPage(link)) {
        System.out.println("Not HTML Page");
        continue;
      }
      HTMLPage currentPage = null;
      // Use the page retriever to get the page
      try {
        currentPage = retriever.getHTMLPage(link);
      }
      catch (PathDisallowedException e) {
        System.out.println(e);
        continue;
      }
      if (currentPage.empty()) {
        System.out.println("No Page Found");
        continue;
      }

      if (currentPage.indexAllowed()) {
        count++;
        System.out.println("Indexing" + "(" + count + "): " + link);
        // store the nodes that are downloaded
        String str = indexHTMLPage(currentPage);
        webGraph.getNode(str);

      }
      if (count < maxCount) {
        List<Link> newLinks = getNewLinks(currentPage);

        // for each node store what its pointing to
        ArrayList<String> neighbors = new ArrayList<String>();

        nodes.put(link.toString(),neighbors );

        // for each node add its neighboring links
        for (Link l : newLinks){
            l.cleanURL();
            neighbors.add(l.toString());
        }

        linksToVisit.addAll(newLinks);
      }
    }

    /*
      Go through all the nodes in the nodes hashmap and
      add an edge in the webGraph
    */
    for (String n : nodes.keySet()){

      // Make sure that there are no repeating edges
      HashSet<String> visitedNodes = new HashSet<String>();

      // for each neighbor of the node in the nodes hashmap
      for (String neighbor : nodes.get(n)){

          // if the node was saved (urlToIndex stores all nodes that are saved)
          // and if the node is not equal to its neighbor or we aren't retrieving
          // its neighbor again add to graph
          if (urlToIndex.get(neighbor) != null && !neighbor.equals(n) && visitedNodes.add(neighbor)){
            webGraph.addEdge(urlToIndex.get(n), urlToIndex.get(neighbor));
          }

      }

    }

    // use this information for the page rank algo
    pageCount = count;

  }



  /**
   * "Indexes" a <code>HTMLpage</code>.  This version just writes it
   * out to a file in the specified directory with a "P<count>.html" file name.
   *
   * @param page An <code>HTMLPage</code> that contains the page to
   *             index.
   */
  public String indexHTMLPage(HTMLPage page) {

    String str = "P" + MoreString.padWithZeros(count, (int)
          Math.floor(MoreMath.log(maxCount, 10)) + 1);

    page.write(saveDir, str);

    urlToIndex.put(page.link.toString(), str);

    return str;
  }

  /*

  This method implements the page rank algorithm.
  It takes in the parameter alpha manipulates the
  final page rank hashmap. At the end of this method
  the values for the hashmap should be the appropriate values.

  */

  public void pageRank(Double alpha){

    // Set the initial page rank to 1/n

    for (Node n : webGraph.nodeArray()){
      pageRanks.put(n.toString(), 1.0/pageCount);
    }

    // run the algorithm 50 times till the page rank stablizes

    for (int i =0; i < 50; i++){

      // initialize a temp map to keep track of new page rank values
      HashMap<String, Double> tempMap = new HashMap<String, Double>();

      // initial edge weight, stays constant
      double edgeVal = alpha/pageCount;

      // normalization value
      double c = 0.0;

      // for all nodes in webGraph array
        for (Node n : webGraph.nodeArray()){

          // initialize the page rank score
          Double val = 0.0;

          // for each parent
          for (Node parent: n.getEdgesIn()){

            // take the existing score and the number of
            // outgoing links that the parent has and
            // add it to the val variable
            Double rank = pageRanks.get(parent.toString());
            int totOutgoingLinks = parent.getEdgesOut().size();

            val += rank/totOutgoingLinks;

          }

          // take the value and multiply it by 1-alpha and add
          // the constant edge val
          val = val * (1-alpha);
          val += edgeVal;

          // for that given page put the value in
          tempMap.put(n.toString(), val);

          // add up all the values
          c += val;
        }

        // invert the values to get the scale
        c = 1.0/c;

        // normalize all the nodes

        for (Node n : webGraph.nodeArray()){

          tempMap.put(n.toString(), c * tempMap.get(n.toString()));

        }

        // set the pageRank hashmap to the one calculated in this iteration
        pageRanks = tempMap;

      }

  }

  /*
    This method prints out the page ranks in a seperate file titles
    page_ranks.txt.
  */
  public void outputPageRanks(){

    // go through the hashmap and put in the value
    // to page_ranks.txt for the directory we are in.
    try(BufferedWriter writer = new BufferedWriter(new FileWriter(saveDir.toString() + "/page_ranks.txt"))){
      for (Map.Entry<String,Double> entry : pageRanks.entrySet()){

        String str = entry.getKey() + ".html " + entry.getValue() + "\n";
        writer.write(str);
      }

      writer.close();

      }catch(IOException e){

      }

  }


  /**
   * Spider the web according to the following command options:
   * <ul>
   * <li>-safe : Check for and obey robots.txt and robots META tag
   * directives.</li>
   * <li>-d &lt;directory&gt; : Store indexed files in &lt;directory&gt;.</li>
   * <li>-c &lt;maxCount&gt; : Store at most &lt;maxCount&gt; files (default is 10,000).</li>
   * <li>-u &lt;url&gt; : Start at &lt;url&gt;.</li>
   * <li>-slow : Pause briefly before getting a page.  This can be
   * useful when debugging.
   * </ul>
   */
  public static void main(String args[])  {
    new PageRankSpider().go(args);
  }
}
