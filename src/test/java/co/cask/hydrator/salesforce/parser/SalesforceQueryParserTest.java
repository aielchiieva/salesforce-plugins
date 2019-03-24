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
package co.cask.hydrator.salesforce.parser;

import co.cask.hydrator.salesforce.SObjectDescriptor;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class SalesforceQueryParserTest {
  private static final Map<String, Object[]> queries = new HashMap<>();

  static {
    queries.put("SELECT Id, Name FROM Opportunity",
                new Object[]{"Opportunity", new String[]{"Id", "Name"}});

    queries.put("SELECT Id FROM Contact WHERE Name LIKE 'A%' AND MailingCity = 'California'",
                new Object[]{"Contact", new String[]{"Id"}});

    queries.put("SELECT Name FROM Account ORDER BY Name DESC NULLS LAST",
                new Object[]{"Account", new String[]{"Name"}});

    queries.put("SELECT Name FROM Account WHERE Industry = 'media' LIMIT 125",
                new Object[]{"Account", new String[]{"Name"}});

    queries.put("SELECT Name FROM Account WHERE Industry = 'media' ORDER BY BillingPostalCode ASC NULLS LAST LIMIT 125",
                new Object[]{"Account", new String[]{"Name"}});

    queries.put("SELECT Name, Id FROM Merchandise__c ORDER BY Name OFFSET 100",
                new Object[]{"Merchandise__c", new String[]{"Name", "Id"}});

    queries.put("SELECT Name, Id FROM Merchandise__c ORDER BY Name LIMIT 20 OFFSET 100",
                new Object[]{"Merchandise__c", new String[]{"Name", "Id"}});

    queries.put("SELECT Id, Name, Account.Name FROM Contact WHERE Account.Industry = 'media'",
                new Object[]{"Contact", new String[]{"Id", "Name", "Account.Name"}});

    queries.put("SELECT Id, FirstName__c, FirstName__c FROM Daughter__c WHERE Mother_of_Child__r.LastName__c LIKE 'C%'",
                new Object[]{"Daughter__c", new String[]{"Id", "FirstName__c", "FirstName__c"}});

    queries.put("SELECT Id, Who.FirstName, Who.LastName FROM Task WHERE Owner.FirstName LIKE 'B%'",
                new Object[]{"Task", new String[]{"Id", "Who.FirstName", "Who.LastName"}});

    queries.put("SELECT UserId, LoginTime from LoginHistory",
                new Object[]{"LoginHistory", new String[]{"UserId", "LoginTime"}});

    queries.put("SELECT LoginHistory.UserId, LoginHistory.LoginTime from LoginHistory",
      new Object[]{"LoginHistory", new String[]{"UserId", "LoginTime"}});

    queries.put("SELECT lh.UserId, lh.LoginTime from LoginHistory lh",
      new Object[]{"LoginHistory", new String[]{"UserId", "LoginTime"}});

    queries.put("SELECT lh.UserId, lh.LoginTime from LoginHistory AS lh",
      new Object[]{"LoginHistory", new String[]{"UserId", "LoginTime"}});

    queries.put("SELECT UserId from LoginHistory WHERE LoginTime > " +
                  "2010-09-20T22:16:30.000Z AND LoginTime < 2010-09-21T22:16:30.000Z",
                new Object[]{"LoginHistory", new String[]{"UserId"}});
  }

  @Test
  public void testGetSObjectFromQuery() {
    for (Map.Entry<String, Object[]> entry : SalesforceQueryParserTest.queries.entrySet()) {
      String query = entry.getKey();
      String expectedSObjectName = (String) entry.getValue()[0];

      SObjectDescriptor actualSObjectName = SalesforceQueryParser.getObjectDescriptorFromQuery(query);
      Assert.assertEquals(expectedSObjectName, actualSObjectName.getName());
    }
  }

  @Test
  public void testGetFieldsFromQuery() {
    for (Map.Entry<String, Object[]> entry : SalesforceQueryParserTest.queries.entrySet()) {
      String query = entry.getKey();
      List<String> expectedFields = Arrays.asList((String[]) entry.getValue()[1]);

      SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromQuery(query);
      List<String> actualFields =  sObjectDescriptor.getFieldsNames();
      Assert.assertEquals(expectedFields, actualFields);
    }
  }

  @Test
  public void testQueryParseError() {
    Stream.of(
      "SELECT COUNT() FROM Contact",
      "SELECT LeadSource, COUNT(Name) FROM Lead GROUP BY LeadSource",
      "SELECT Name, COUNT(Id) FROM Account GROUP BY Name HAVING COUNT(Id) > 1",
      "SELECT NAME n from Account",
      "SELECT * from Account",
      "SELECT Name, (SELECT LastName FROM Contacts) FROM Account",
      "SELECT TYPEOF What WHEN Account THEN Phone ELSE Email END FROM Event",
      "SELECT Name FROM Contact.Account")
      .forEach(query -> {
        try {
          SObjectDescriptor.fromQuery(query);
          Assert.fail();
        } catch (SOQLParsingException e) {
          // expected failure, do nothing
        }
      });
  }

}
