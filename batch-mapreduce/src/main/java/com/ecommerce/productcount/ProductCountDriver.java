package com.ecommerce.productcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * ProductCountDriver — Job 1 (ProductCount)
 *
 * Pilote le job MapReduce qui calcule la quantité totale vendue par produit.
 *
 * Exécution locale (IntelliJ) :
 *   args = ["data/ecommerce_sales_34500.csv", "output/product_count"]
 *
 * Exécution sur le cluster :
 *   hadoop jar ecommerce-batch-1.0.jar \
 *       com.ecommerce.productcount.ProductCountDriver \
 *       /user/root/input/ecommerce_sales_34500.csv \
 *       /user/root/output/product_count
 */
public class ProductCountDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ProductCountDriver <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Product Count (quantite vendue par produit)");

        job.setJarByClass(ProductCountDriver.class);
        job.setMapperClass(ProductMapper.class);
        // Combiner = Reducer : optimisation réseau (agrégation locale)
        job.setCombinerClass(ProductReducer.class);
        job.setReducerClass(ProductReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
