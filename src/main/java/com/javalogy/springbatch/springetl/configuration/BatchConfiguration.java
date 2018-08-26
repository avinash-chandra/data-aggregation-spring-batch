package com.javalogy.springbatch.springetl.configuration;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import com.javalogy.springbatch.springetl.listeners.JobCompletionNotificationListener;
import com.javalogy.springbatch.springetl.model.Account;
import com.javalogy.springbatch.springetl.processor.AccountDataProcessor;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    // tag::readerwriterprocessor[]
    @Bean
	public FlatFileItemReader<Account> reader() {
        return new FlatFileItemReaderBuilder<Account>()
            .name("accountItemReader")
            .resource(new ClassPathResource("sample-data.csv"))
            .delimited()
            .names(new String[]{"firstName", "lastName"})
            .fieldSetMapper(new BeanWrapperFieldSetMapper<Account>() {{
                setTargetType(Account.class);
            }})
            .build();
    }

    @Bean
    public AccountDataProcessor processor() {
        return new AccountDataProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<Account> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Account>()
            .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
            .sql("INSERT INTO account (first_name, last_name) VALUES (:firstName, :lastName)")
            .dataSource(dataSource)
            .build();
    }
    // end::readerwriterprocessor[]
    
    /*
     * Jobs are built from steps, where each step can involve a reader, a processor, and a writer.
     * In the step definition, you define how much data to write at a time. 
     * In this case, it writes up to ten records at a time. Next, you configure the reader, processor, and writer using the injected bits from earlier.
     * 
     */

    @Bean
    public Step step1(JdbcBatchItemWriter<Account> writer) {
        return stepBuilderFactory.get("step1").<Account, Account> chunk(10).reader(reader()).processor(processor()).writer(writer).build();
    }

    /*
     * In this job definition, you need an incrementer because jobs use a database to maintain execution state. 
     * You then list each step, of which this job has only one step. The job ends, and the Java API produces a perfectly configured job.
     * 
     * 
     */
    @Bean
    public Job importUserJob(JobCompletionNotificationListener listener, Step step1) {
        return jobBuilderFactory.get("importUserJob")
            .incrementer(new RunIdIncrementer())
            .listener(listener)
            .flow(step1)
            .end()
            .build();
    }
}