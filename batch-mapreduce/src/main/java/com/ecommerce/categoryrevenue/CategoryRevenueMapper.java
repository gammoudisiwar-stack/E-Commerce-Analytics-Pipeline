package com.ecommerce.categoryrevenue;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * CategoryRevenueMapper — Job 2 (CategoryRevenue)
 *
 * Émet la paire (product_category, total_amount) pour chaque transaction.
 *
 * Schéma CSV (index) :
 *   3=category ... 12=total_amount
 */
public class CategoryRevenueMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private final Text category = new Text();
    private final DoubleWritable amount = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Ignorer l'en-tête
        if (key.get() == 0 && line.startsWith("order_id")) {
            return;
        }

        String[] fields = line.split(",");
        if (fields.length < 13) {
            return;
        }

        try {
            String cat = fields[3].trim();              // category
            double total = Double.parseDouble(fields[12].trim()); // total_amount

            if (cat.isEmpty()) {
                return;
            }

            category.set(cat);
            amount.set(total);
            context.write(category, amount);
        } catch (NumberFormatException e) {
            context.getCounter("CategoryRevenue", "MALFORMED_ROWS").increment(1);
        }
    }
}
