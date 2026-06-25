package com.ecommerce.categoryrevenue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * CategoryRevenueDriver — Job 2 (CategoryRevenue)
 *
 * Pilote le job MapReduce qui calcule le chiffre d'affaires total
 * par catégorie de produit.
 *
 * Exécution locale (IntelliJ) :
 *   args = ["data/ecommerce_sales_34500.csv", "output/category_revenue"]
 *
 * Exécution sur le cluster :
 *   hadoop jar ecommerce-batch-1.0.jar \
 *       com.ecommerce.categoryrevenue.CategoryRevenueDriver \
 *       /user/root/input/ecommerce_sales_34500.csv \
 *       /user/root/output/category_revenue
 */
public class CategoryRevenueDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CategoryRevenueDriver <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Category Revenue (chiffre d'affaires par categorie)");

        job.setJarByClass(CategoryRevenueDriver.class);
        job.setMapperClass(CategoryRevenueMapper.class);
        job.setCombinerClass(CategoryRevenueReducer.class);
        job.setReducerClass(CategoryRevenueReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
