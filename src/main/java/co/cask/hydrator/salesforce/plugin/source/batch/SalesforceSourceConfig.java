/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.hydrator.salesforce.plugin.source.batch;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.etl.api.validation.InvalidConfigPropertyException;
import co.cask.hydrator.salesforce.SObjectDescriptor;
import co.cask.hydrator.salesforce.parser.SalesforceQueryParser;
import co.cask.hydrator.salesforce.plugin.BaseSalesforceConfig;
import co.cask.hydrator.salesforce.plugin.source.batch.util.SalesforceQueryUtil;
import co.cask.hydrator.salesforce.plugin.source.batch.util.SalesforceSourceConstants;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.sforce.ws.ConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * This class {@link SalesforceSourceConfig} provides all the configuration required for
 * configuring the {@link SalesforceBatchSource} plugin.
 */
public class SalesforceSourceConfig extends BaseSalesforceConfig {

  private static final Logger LOG = LoggerFactory.getLogger(SalesforceSourceConfig.class);

  @Name(SalesforceSourceConstants.PROPERTY_QUERY)
  @Description("The SOQL query to retrieve results from. Example: select Id, Name from Opportunity")
  @Nullable
  @Macro
  private String query;

  @Name(SalesforceSourceConstants.PROPERTY_SOBJECT_NAME)
  @Description("Salesforce SObject name. Example: Opportunity")
  @Nullable
  @Macro
  private String sObjectName;

  @Name(SalesforceSourceConstants.PROPERTY_DATETIME_FILTER)
  @Description("Salesforce SObject query datetime filter. Example: 2019-03-12T11:29:52Z, LAST_WEEK")
  @Nullable
  @Macro
  private String datetimeFilter;

  @Name(SalesforceSourceConstants.PROPERTY_DURATION)
  @Description("Salesforce SObject query duration. Default value: 0")
  @Nullable
  @Macro
  private Integer duration;

  @Name(SalesforceSourceConstants.PROPERTY_OFFSET)
  @Description("Salesforce SObject query offset. Default value: 0")
  @Nullable
  @Macro
  private Integer offset;


  @VisibleForTesting
  SalesforceSourceConfig(String referenceName,
                                String clientId,
                                String clientSecret,
                                String username,
                                String password,
                                String loginUrl,
                                @Nullable String query,
                                @Nullable String sObjectName,
                                @Nullable String datetimeFilter,
                                @Nullable Integer duration,
                                @Nullable Integer offset) {
    super(referenceName, clientId, clientSecret, username, password, loginUrl);
    this.query = query;
    this.sObjectName = sObjectName;
    this.datetimeFilter = datetimeFilter;
    this.duration = duration;
    this.offset = offset;
  }

  public String getQuery() {
    if (isSoqlQuery()) {
      return query;
    }
    return getSObjectQuery();
  }

  @Nullable
  public String getSObjectName() {
    return sObjectName;
  }

  public int getDuration() {
    return Objects.isNull(duration) ? 0 : duration;
  }

  public int getOffset() {
    return Objects.isNull(offset) ? 0 : offset;
  }

  @Nullable
  public String getDatetimeFilter() {
    return datetimeFilter;
  }

  public boolean isSoqlQuery() {
    if (!Strings.isNullOrEmpty(query)) {
      return true;
    } else if (!Strings.isNullOrEmpty(sObjectName)) {
      return false;
    }
    throw new InvalidConfigPropertyException("SOQL query or SObject Name must be provided",
                                             SalesforceSourceConstants.PROPERTY_QUERY);
  }

  @Override
  public void validate() {
    super.validate();
    if (!containsMacro(SalesforceSourceConstants.PROPERTY_QUERY) && !Strings.isNullOrEmpty(query)) {
      try {
        SalesforceQueryParser.validateQuery(query);
      } catch (Exception e) {
        throw new InvalidConfigPropertyException(String.format("Invalid SOQL query: '%s", query), e,
                                                 SalesforceSourceConstants.PROPERTY_QUERY);
      }
    }
    if (!containsMacro(SalesforceSourceConstants.PROPERTY_QUERY)
      && !containsMacro(SalesforceSourceConstants.PROPERTY_SOBJECT_NAME)) {
      if (!isSoqlQuery()) {
        validateSObjectFilter(SalesforceSourceConstants.PROPERTY_DURATION, getDuration(),
                              "Invalid SObject duration value: '%d'. Duration value must be '%d' or greater");
        validateSObjectFilter(SalesforceSourceConstants.PROPERTY_OFFSET, getOffset(),
                              "Invalid SObject offset value: '%d'. Offset value must be '%d' or greater");
      }
    }
  }

  private void validateSObjectFilter(String propertyDuration, int value, String message) {
    if (!containsMacro(propertyDuration)) {
      int minValue = 0;
      if (value < minValue) {
        throw new InvalidConfigPropertyException(String.format(message, value, minValue), propertyDuration);
      }
    }
  }

  private String getSObjectQuery() {
    try {
      SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromName(sObjectName, getAuthenticatorCredentials());
      String sObjectQuery = SalesforceQueryUtil.createSObjectQuery(sObjectDescriptor.getFieldsNames(), sObjectName,
                                                                   getDuration(), getOffset(), datetimeFilter);
      LOG.debug("Generated SObject query: '{}'", sObjectQuery);
      return sObjectQuery;
    } catch (ConnectionException e) {
      throw new IllegalStateException(
        String.format("Cannot establish connection to Salesforce to describe SObject: '%s'", sObjectName));
    }
  }
}
