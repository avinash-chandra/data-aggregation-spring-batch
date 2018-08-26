package com.javalogy.springbatch.springetl.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import com.javalogy.springbatch.springetl.model.Account;

/*
 * A common paradigm in batch processing is to upload data, transform it, and then pipe it out somewhere else. 
 * Here you write a simple transformer that converts the names to upper-case
 * 
 */

/*
 * There is no requirement that the input and output types be the same. 
 * In fact, after one source of data is read, sometimes the application’s data flow needs a different data type.
 * 
 */

public class AccountDataProcessor implements ItemProcessor<Account, Account> {

    private static final Logger log = LoggerFactory.getLogger(AccountDataProcessor.class);

    @Override
    public Account process(final Account account) throws Exception {
        final String firstName = account.getFirstName().toUpperCase();
        final String lastName = account.getLastName().toUpperCase();

        final Account transformedAccount = new Account(firstName, lastName);

        log.info("Converting (" + account + ") into (" + transformedAccount + ")");

        return transformedAccount;
    }

}