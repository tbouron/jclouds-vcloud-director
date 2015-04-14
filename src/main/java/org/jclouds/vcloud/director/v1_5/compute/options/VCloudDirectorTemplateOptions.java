/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jclouds.vcloud.director.v1_5.compute.options;

import static com.google.common.base.Objects.equal;

import com.google.common.base.Objects;

import org.jclouds.compute.options.TemplateOptions;
import org.jclouds.scriptbuilder.domain.Statement;
import org.jclouds.scriptbuilder.domain.Statements;

public class VCloudDirectorTemplateOptions extends TemplateOptions implements Cloneable {

   private Statement guestCustomizationScript = null;

   /**
    * Specifies a script to be added to the GuestCustomizationSection
    */
   public VCloudDirectorTemplateOptions guestCustomizationScript(String guestCustomizationScript) {
      return guestCustomizationScript(Statements.exec(guestCustomizationScript));
   }

   /**
    * Specifies a script to be added to the GuestCustomizationSection
    */
   public VCloudDirectorTemplateOptions guestCustomizationScript(Statement guestCustomizationScript) {
      this.guestCustomizationScript = guestCustomizationScript;
      return this;
   }

   public Statement getGuestCustomizationScript() {
      return guestCustomizationScript;
   }

   @Override
   public VCloudDirectorTemplateOptions clone() {
      VCloudDirectorTemplateOptions options = new VCloudDirectorTemplateOptions();
      copyTo(options);
      return options;
   }

   @Override
   public void copyTo(TemplateOptions to) {
      super.copyTo(to);
      if (to instanceof VCloudDirectorTemplateOptions) {
         VCloudDirectorTemplateOptions vto = VCloudDirectorTemplateOptions.class.cast(to);
         if (getGuestCustomizationScript() != null)
            vto.guestCustomizationScript(getGuestCustomizationScript());
      }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o)
         return true;
      if (o == null || getClass() != o.getClass())
         return false;
      VCloudDirectorTemplateOptions that = VCloudDirectorTemplateOptions.class.cast(o);
      return super.equals(that) && equal(this.guestCustomizationScript, that.guestCustomizationScript);
   }

   @Override
   public int hashCode() {
      return Objects.hashCode(super.hashCode(), guestCustomizationScript);
   }

   @Override
   public Objects.ToStringHelper string() {
      Objects.ToStringHelper toString = super.string();
      if (guestCustomizationScript != null)
         toString.add("guestCustomizationScript", guestCustomizationScript);
      return toString;
   }

   /**
    * @see VCloudDirectorTemplateOptions#guestCustomizationScript
    */
   public static class Builder extends TemplateOptions.Builder {
      public static VCloudDirectorTemplateOptions guestCustomizationScript(String guestCustomizationScript) {
         return guestCustomizationScript(Statements.exec(guestCustomizationScript));
      }

      public static VCloudDirectorTemplateOptions guestCustomizationScript(Statement guestCustomizationScript) {
         VCloudDirectorTemplateOptions options = new VCloudDirectorTemplateOptions();
         return options.guestCustomizationScript(guestCustomizationScript);
      }
   }
}
