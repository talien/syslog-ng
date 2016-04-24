/*
 * Copyright (c) 2015 Balabit
 * Copyright (c) 2015 Viktor Juhasz <viktor.juhasz@balabit.com>
 * Copyright (c) 2015 Viktor Tusa <tusavik@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published
 * by the Free Software Foundation, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 * As an additional exemption you are allowed to compile & link against the
 * OpenSSL libraries as published by the OpenSSL project. See the file
 * COPYING for details.
 *
 */

package org.syslog_ng.elasticsearch_http;

import org.syslog_ng.LogMessage;
import org.syslog_ng.LogTemplate;
import org.syslog_ng.StructuredLogDestination;
import org.syslog_ng.elasticsearch_http.ElasticSearchHTTPOptions;
import org.syslog_ng.elasticsearch_http.awssigner.AWSSigner;
import org.syslog_ng.elasticsearch_http.awssigner.AWSSigningRequestInterceptor;
import org.syslog_ng.options.InvalidOptionException;
import org.syslog_ng.logging.SyslogNgInternalLogger;
import org.apache.log4j.Logger;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestClient;
import io.searchbox.core.Index;
import io.searchbox.client.JestResult;
import io.searchbox.cluster.NodesInfo;
import io.searchbox.client.config.HttpClientConfig;
import org.syslog_ng.elasticsearch_http.messageprocessor.ESHTTPSingleMessageProcessor;
import org.syslog_ng.elasticsearch_http.messageprocessor.ESHTTPBulkMessageProcessor;
import org.syslog_ng.elasticsearch_http.messageprocessor.ESHTTPMessageProcessor;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.AWSCredentialsProvider;

import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;

import org.joda.time.LocalDateTime;

import java.util.Set;
import java.util.LinkedHashSet;

import java.io.IOException;

public class ElasticSearchHTTPDestination extends StructuredLogDestination {

    private class CurrentTimeProvider implements AWSSigner.CurrentTimeProvider {
        public LocalDateTime getTime()
        {
            //return LocalDateTime.of(2011, 9, 9, 23, 36, 0);
            return LocalDateTime.now();
        }
    }

	JestClient client;
	ElasticSearchHTTPOptions options;
	Logger logger;
	ESHTTPMessageProcessor messageProcessor;

	public ElasticSearchHTTPDestination(long handle) {
		super(handle);
		logger = Logger.getRootLogger();
		SyslogNgInternalLogger.register(logger);
		options = new ElasticSearchHTTPOptions(this);
	}

	private ESHTTPMessageProcessor createMessageProcessor()
	{
		if (options.getFlushLimit() > 1)
			return new ESHTTPBulkMessageProcessor(options, client);
		else
			return new ESHTTPSingleMessageProcessor(options, client);
	}

    private JestClientFactory createAWSClient()
    {
        final AWSCredentialsProvider awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
        final AWSSigner awsSigner = new AWSSigner(awsCredentialsProvider, "us-east-1", "es", new CurrentTimeProvider());
        final AWSSigningRequestInterceptor requestInterceptor = new AWSSigningRequestInterceptor(awsSigner);
        final JestClientFactory factory = new JestClientFactory() {
            @Override
            protected HttpClientBuilder configureHttpClient(HttpClientBuilder builder) {
                builder.addInterceptorLast(requestInterceptor);
                return builder;
            }
            @Override
            protected HttpAsyncClientBuilder configureHttpClient(HttpAsyncClientBuilder builder) {
                builder.addInterceptorLast(requestInterceptor);
                return builder;
            }
        };
        logger.debug("Building AWS Jest client");
        return factory;   
    }

    private JestClient createClient() {
            JestClient result;
            JestClientFactory clientFactory;
			String connectionUrl = options.getClusterUrl();
            logger.debug("Creating JestClient, client_mode=" + options.getClientMode());
            if (options.getClientMode().equals(ElasticSearchHTTPOptions.CLIENT_MODE_AWS)) { 
                clientFactory = createAWSClient();
            } else {
			    clientFactory = new JestClientFactory(); 
            }
			clientFactory.setHttpClientConfig(new HttpClientConfig
			       .Builder(connectionUrl)
			       .multiThreaded(false)
			       .build());
			result = clientFactory.getObject();
            return result;

    }

	@Override
	protected boolean init() {
		boolean result = false;
		logger.debug("Initializing ESHTTP Destination");
		try {
			options.init();
            client = createClient();
			messageProcessor = createMessageProcessor();
			messageProcessor.init();
			getClusterName();
			result = true;
		}
		catch (Throwable e){
			logger.error(e.getMessage());
			logger.error(e.getClass().getName());
			return false;
		}
		return result;
	}

	@Override
	protected boolean isOpened() {
		return true;
	}

	@Override
	protected boolean open() {
		return true;
	}

	private Index createIndex(LogMessage msg) {
		String formattedMessage = options.getMessageTemplate().getResolvedString(msg, getTemplateOptionsHandle(), LogTemplate.LTZ_SEND);
		String customId = options.getCustomId().getResolvedString(msg, getTemplateOptionsHandle(), LogTemplate.LTZ_SEND);
		String indexName = options.getIndex().getResolvedString(msg, getTemplateOptionsHandle(), LogTemplate.LTZ_SEND);
		String type = options.getType().getResolvedString(msg, getTemplateOptionsHandle(), LogTemplate.LTZ_SEND);
		logger.debug("Outgoing log entry, json='" + formattedMessage + "'");
		Index index = new Index.Builder(formattedMessage).index(indexName).type(type).id(customId).build();
		return index;
	}

	@Override
	protected boolean send(LogMessage msg) {
		Index index = createIndex(msg);
	 	boolean result = messageProcessor.send(index);	
		return result;
	}
	

	@Override
	protected void close() {
		messageProcessor.flush();
	}

        private String getClusterName() {
		try {
			NodesInfo nodesinfo = new NodesInfo.Builder().build();
			JestResult result = client.execute(nodesinfo);
			return result.getValue("cluster_name").toString();
		} 
		catch (IOException e) {
			return "unnamed";
		}
		
	}

	@Override
	protected String getNameByUniqOptions() {
		return String.format("ElasticSearch,%s,%s", getClusterName(), options.getIndex().getValue());
	}

	@Override
	protected void deinit() {
		client.shutdownClient();
		options.deinit();
	}
}
