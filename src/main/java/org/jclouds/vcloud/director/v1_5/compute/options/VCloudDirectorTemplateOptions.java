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
import java.util.Map;

import org.jclouds.compute.options.TemplateOptions;
import org.jclouds.domain.LoginCredentials;
import org.jclouds.javax.annotation.Nullable;
import org.jclouds.scriptbuilder.domain.Statement;
import org.jclouds.scriptbuilder.domain.Statements;

import com.google.common.base.Objects;
import com.google.common.base.Optional;

public class VCloudDirectorTemplateOptions extends TemplateOptions implements Cloneable {

   private Statement guestCustomizationScript = null;
   protected Optional<String> memory = Optional.absent();
   protected Optional<String> virtualCpus = Optional.absent();

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

   public Optional<String> getMemory() { return memory; }

   public Optional<String> getVirtualCpus() { return virtualCpus; }

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
         if (memory.isPresent()) {
            vto.memory(memory.get());
         }
         if (virtualCpus.isPresent()) {
            vto.virtualCpus(virtualCpus.get());
         }
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

   public VCloudDirectorTemplateOptions memory(@Nullable String memory) {
      this.memory = Optional.fromNullable(memory);
      return this;
   }

   public VCloudDirectorTemplateOptions virtualCpus(@Nullable String virtualCpus) {
      this.virtualCpus = Optional.fromNullable(virtualCpus);
      return this;
   }

   // methods that only facilitate returning the correct object type

   /**
    * {@inheritDoc}
    */
   @Override
   public VCloudDirectorTemplateOptions blockOnPort(int port, int seconds) {
      return VCloudDirectorTemplateOptions.class.cast(super.blockOnPort(port, seconds));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public VCloudDirectorTemplateOptions inboundPorts(int... ports) {
      return VCloudDirectorTemplateOptions.class.cast(super.inboundPorts(ports));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public VCloudDirectorTemplateOptions authorizePublicKey(String publicKey) {
      return VCloudDirectorTemplateOptions.class.cast(super.authorizePublicKey(publicKey));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public VCloudDirectorTemplateOptions installPrivateKey(String privateKey) {
      return VCloudDirectorTemplateOptions.class.cast(super.installPrivateKey(privateKey));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public VCloudDirectorTemplateOptions blockUntilRunning(boolean blockUntilRunning) {
      return VCloudDirectorTemplateOptions.class.cast(super.blockUntilRunning(blockUntilRunning));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public VCloudDirectorTemplateOptions dontAuthorizePublicKey() {
      return VCloudDirectorTemplateOptions.class.cast(super.dontAuthorizePublicKey());
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public VCloudDirectorTemplateOptions nameTask(String name) {
      return VCloudDirectorTemplateOptions.class.cast(super.nameTask(name));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public VCloudDirectorTemplateOptions runAsRoot(boolean runAsRoot) {
      return VCloudDirectorTemplateOptions.class.cast(super.runAsRoot(runAsRoot));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public VCloudDirectorTemplateOptions runScript(Statement script) {
      return VCloudDirectorTemplateOptions.class.cast(super.runScript(script));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public VCloudDirectorTemplateOptions overrideLoginCredentials(LoginCredentials overridingCredentials) {
      return VCloudDirectorTemplateOptions.class.cast(super.overrideLoginCredentials(overridingCredentials));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public VCloudDirectorTemplateOptions overrideLoginPassword(String password) {
      return VCloudDirectorTemplateOptions.class.cast(super.overrideLoginPassword(password));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public VCloudDirectorTemplateOptions overrideLoginPrivateKey(String privateKey) {
      return VCloudDirectorTemplateOptions.class.cast(super.overrideLoginPrivateKey(privateKey));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public VCloudDirectorTemplateOptions overrideLoginUser(String loginUser) {
      return VCloudDirectorTemplateOptions.class.cast(super.overrideLoginUser(loginUser));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public VCloudDirectorTemplateOptions overrideAuthenticateSudo(boolean authenticateSudo) {
      return VCloudDirectorTemplateOptions.class.cast(super.overrideAuthenticateSudo(authenticateSudo));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public VCloudDirectorTemplateOptions userMetadata(Map<String, String> userMetadata) {
      return VCloudDirectorTemplateOptions.class.cast(super.userMetadata(userMetadata));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public VCloudDirectorTemplateOptions userMetadata(String key, String value) {
      return VCloudDirectorTemplateOptions.class.cast(super.userMetadata(key, value));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public VCloudDirectorTemplateOptions nodeNames(Iterable<String> nodeNames) {
      return VCloudDirectorTemplateOptions.class.cast(super.nodeNames(nodeNames));
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public VCloudDirectorTemplateOptions networks(Iterable<String> networks) {
      return VCloudDirectorTemplateOptions.class.cast(super.networks(networks));
   }

}
