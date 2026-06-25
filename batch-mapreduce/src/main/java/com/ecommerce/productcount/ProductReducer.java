package com.ecommerce.productcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * ProductReducer — Job 1 (ProductCount)
 *
 * Reçoit pour chaque product_id la liste de toutes les quantités vendues
 * et les additionne pour obtenir la quantité totale vendue par produit.
 *
 * Sortie : (product_id, quantité_totale_vendue)
 */
public class ProductReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable result = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
