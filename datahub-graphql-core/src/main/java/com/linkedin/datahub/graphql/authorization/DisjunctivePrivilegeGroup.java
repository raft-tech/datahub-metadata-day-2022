package com.linkedin.datahub.graphql.authorization;

import java.util.List;

/**
 * Represents a group of privilege groups, any of which must be authorized to authorize a request.
 *
 * That is, an OR of privilege groups.
 */
public class DisjunctivePrivilegeGroup {
  private final List<ConjunctivePrivilegeGroup> _authorizedPrivilegeGroups;

  public DisjunctivePrivilegeGroup(List<ConjunctivePrivilegeGroup> authorizedPrivilegeGroups) {
    _authorizedPrivilegeGroups = authorizedPrivilegeGroups;
  }

  public List<ConjunctivePrivilegeGroup> getAuthorizedPrivilegeGroups() {
    return _authorizedPrivilegeGroups;
  }
}