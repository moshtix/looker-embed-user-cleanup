import fetch from "node-fetch";

// Sleep utility function to add delay between API calls
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Configuration
const CONFIG = {
  // Default delay between API calls in milliseconds
  API_DELAY_MS: 10,
  // Maximum number of retries for API calls
  MAX_RETRIES: 3,
  // Initial retry delay in milliseconds (will be multiplied by 2^retry for exponential backoff)
  RETRY_DELAY_MS: 100,
  // These would typically be loaded from a config file or environment variables
  // For this example, they'll be passed as command line arguments
  BASE_URL: "",
  CLIENT_ID: "",
  CLIENT_SECRET: "",
  // Whether to actually delete users or just simulate (dry run)
  DRY_RUN: true
};

// Helper function to retry API calls with exponential backoff
async function retryFetch(url, options, retries = CONFIG.MAX_RETRIES) {
  try {
    return await fetch(url, options);
  } catch (error) {
    if (retries <= 0) {
      throw error;
    }
    
    console.log(`Connection error: ${error.message}`);
    console.log(`Retrying in ${CONFIG.RETRY_DELAY_MS * Math.pow(2, CONFIG.MAX_RETRIES - retries)}ms... (${retries} retries left)`);
    
    // Wait with exponential backoff
    await sleep(CONFIG.RETRY_DELAY_MS * Math.pow(2, CONFIG.MAX_RETRIES - retries));
    
    // Retry the request
    return retryFetch(url, options, retries - 1);
  }
}

// Helper function to parse command line arguments
function parseArgs() {
  const args = process.argv.slice(2);
  let i = 0;
  
  while (i < args.length) {
    const arg = args[i];
    
    if (arg === "--base-url" && i + 1 < args.length) {
      CONFIG.BASE_URL = args[i + 1];
      i += 2;
    } else if (arg === "--client-id" && i + 1 < args.length) {
      CONFIG.CLIENT_ID = args[i + 1];
      i += 2;
    } else if (arg === "--client-secret" && i + 1 < args.length) {
      CONFIG.CLIENT_SECRET = args[i + 1];
      i += 2;
    } else if (arg === "--delay" && i + 1 < args.length) {
      CONFIG.API_DELAY_MS = parseInt(args[i + 1], 10);
      i += 2;
    } else if (arg === "--force") {
      CONFIG.DRY_RUN = false;
      i++;
    } else {
      i++;
    }
  }
  
  // Validate required configuration
  if (!CONFIG.BASE_URL || !CONFIG.CLIENT_ID || !CONFIG.CLIENT_SECRET) {
    console.error("Error: Missing required configuration.");
    console.error("Usage: node looker-delete-users.js --base-url <url> --client-id <id> --client-secret <secret> [--delay <ms>] [--force]");
    console.error("Note: Add --force to actually delete users. Without this flag, the script runs in dry-run mode.");
    process.exit(1);
  }
}

// Helper function to process a batch of users
async function processUsers(users, client, stats, scheduledPlanOwners) {
  for (const user of users) {
    try {
      console.log(`Processing user: ${user.display_name} (${user.email})`);
      
      // Check if user owns any scheduled plans
      if (scheduledPlanOwners.has(user.id)) {
        console.log(`  User owns scheduled plans, skipping`);
        stats.hasScheduledPlans++;
        continue;
      }

      if (CONFIG.DRY_RUN) {
        console.log(`  DRY RUN: Would delete user ${user.id} (${user.email})`);
        stats.wouldDelete++;
      } else {
        // Delete the user
        await client.deleteUser(user.id);
        stats.deleted++;
        console.log(`  User successfully deleted`);
      }
    } catch (error) {
      console.error(`Error processing user ${user.id}:`, error.message);
      stats.errors++;
    }
  }
}

// Looker API client
class LookerClient {
  constructor(baseUrl, clientId, clientSecret) {
    this.baseUrl = baseUrl.endsWith("/") ? baseUrl.slice(0, -1) : baseUrl;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.accessToken = null;
    this.tokenExpiry = null;
  }
  
  // Get all scheduled plans with pagination
  async getAllScheduledPlans(pageSize = 50, offset = 0) {
    try {
      console.log(`Fetching scheduled plans: offset=${offset}, limit=${pageSize}`);
      
      const headers = await this.getHeaders();
      const url = this.apiUrl(`scheduled_plans?all_users=true&limit=${pageSize}&offset=${offset}`);
      
      const response = await retryFetch(url, {
        method: "GET",
        headers
      });
      
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Failed to get scheduled plans: ${response.status} ${response.statusText} - ${errorText}`);
      }
      
      const plans = await response.json();
      console.log(`Retrieved ${plans.length} scheduled plans`);
      
      return {
        plans: plans,
        hasMore: plans.length === pageSize
      };
    } catch (error) {
      console.error("Error getting scheduled plans:", error.message);
      throw error;
    }
  }

  // Get the API URL
  apiUrl(endpoint) {
    return `${this.baseUrl}/api/4.0/${endpoint}`;
  }
  
  // Get headers with authentication
  async getHeaders() {
    await this.ensureAuthenticated();
    
    return {
      "Authorization": `token ${this.accessToken}`,
      "Content-Type": "application/json"
    };
  }
  
  // Ensure we have a valid access token
  async ensureAuthenticated() {
    const now = new Date();
    
    // If token is expired or will expire in the next minute, refresh it
    if (!this.accessToken || !this.tokenExpiry || this.tokenExpiry <= new Date(now.getTime() + 60000)) {
      await this.authenticate();
    }
  }
  
  // Authenticate with Looker API
  async authenticate() {
    try {
      // Create form data with client_id and client_secret
      const formData = new URLSearchParams();
      formData.append('client_id', this.clientId);
      formData.append('client_secret', this.clientSecret);
      
      const response = await retryFetch(this.apiUrl("login"), {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded"
        },
        body: formData
      });
      
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Authentication failed: ${response.status} ${response.statusText} - ${errorText}`);
      }
      
      const data = await response.json();
      this.accessToken = data.access_token;
      
      // Set token expiry (token typically expires in 1 hour)
      const now = new Date();
      this.tokenExpiry = new Date(now.getTime() + (data.expires_in * 1000));
      
      console.log("Successfully authenticated with Looker API");
    } catch (error) {
      console.error("Authentication error:", error.message);
      throw error;
    }
  }
  
  // Get users with pagination using the search endpoint
  async getUsers(pageSize = 50, offset = 0) {
    try {
      // Include fields we need
      const fields = "id,display_name,email";
      
      console.log(`Fetching users: offset=${offset}, limit=${pageSize}`);
      
      const headers = await this.getHeaders();
      // Use the search endpoint with filters to get only users
      const url = this.apiUrl(`users/search?embed_user=true&limit=${pageSize}&offset=${offset}&fields=${fields}&sorts=id`);
      
      const response = await retryFetch(url, {
        method: "GET",
        headers
      });
      
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Failed to get users: ${response.status} ${response.statusText} - ${errorText}`);
      }
      
      const users = await response.json();
      console.log(`Retrieved ${users.length} users`);
      
      return {
        users: users,
        hasMore: users.length === pageSize
      };
    } catch (error) {
      console.error("Error getting users:", error.message);
      throw error;
    }
  }
  
  // Delete a user
  async deleteUser(userId) {
    try {
      // Add delay before API call to avoid rate limiting
      await sleep(CONFIG.API_DELAY_MS);
      
      const headers = await this.getHeaders();
      const response = await retryFetch(this.apiUrl(`users/${userId}`), {
        method: "DELETE",
        headers
      });
      
      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Failed to delete user ${userId}: ${response.status} ${response.statusText} - ${errorText}`);
      }
      
      return true;
    } catch (error) {
      console.error(`Error deleting user ${userId}:`, error.message);
      throw error;
    }
  }
}

// Main function
async function main() {
  try {
    // Parse command line arguments
    parseArgs();
    
    if (CONFIG.DRY_RUN) {
      console.log("RUNNING IN DRY-RUN MODE. No users will be deleted.");
      console.log("Add --force to actually delete users.");
      console.log("");
    } else {
      console.log("WARNING: RUNNING IN FORCE MODE. Users will be permanently deleted!");
      console.log("");
      
      // Add a 5-second delay to give the user a chance to cancel
      console.log("Starting in 5 seconds...");
      await sleep(5000);
    }
    
    console.log("Looking for users to delete...");
    
    // Initialize Looker client
    const client = new LookerClient(CONFIG.BASE_URL, CONFIG.CLIENT_ID, CONFIG.CLIENT_SECRET);
    
    // Track statistics
    const stats = {
      total: 0,
      deleted: 0,
      wouldDelete: 0,
      hasScheduledPlans: 0,
      errors: 0
    };
    
    // Fetch all scheduled plans to identify users who own plans
    console.log("Fetching all scheduled plans to identify users who own plans...");
    const scheduledPlanOwners = new Set();
    let plansOffset = 0;
    let plansHasMore = true;
    let totalPlans = 0;
    
    while (plansHasMore) {
      // Get a batch of scheduled plans
      const plansResult = await client.getAllScheduledPlans(50, plansOffset);
      const plans = plansResult.plans;
      
      // Add user IDs to the set
      for (const plan of plans) {
        if (plan.user_id) {
          scheduledPlanOwners.add(plan.user_id);
        }
      }
      
      totalPlans += plans.length;
      plansOffset += 50;
      plansHasMore = plansResult.hasMore;
      
      // Add delay between batches
      if (plansHasMore) {
        console.log(`Waiting ${CONFIG.API_DELAY_MS}ms before next batch of plans...`);
        await sleep(CONFIG.API_DELAY_MS);
      }
    }
    
    console.log(`Found ${totalPlans} scheduled plans owned by ${scheduledPlanOwners.size} unique users`);
    

    // Process users in batches
    const pageSize = 50;
    let offset = 0;
    let hasMore = true;
    
    while (hasMore) {
      // Get a batch of users
      const result = await client.getUsers(pageSize, offset);
      const users = result.users;
      
      // Update total count
      stats.total += users.length;
      
      // Process this batch of users
      await processUsers(users, client, stats, scheduledPlanOwners);
      
      // Move to the next batch
      offset += pageSize;
      hasMore = result.hasMore;
      
      // Add delay between batches
      if (hasMore) {
        console.log(`Waiting ${CONFIG.API_DELAY_MS}ms before next batch...`);
        await sleep(CONFIG.API_DELAY_MS);
      }
    }
    
    // Print summary
    console.log("\n-----------------------------");
    console.log("---------- Summary ----------");
    console.log("-----------------------------");
    console.log(` Total users: ${stats.total}`);
    console.log(` Users with scheduled plans: ${stats.hasScheduledPlans}`);
    if (CONFIG.DRY_RUN) {
      console.log(` Users that would be deleted: ${stats.wouldDelete}`);
    } else {
      console.log(` Users deleted: ${stats.deleted}`);
    }
    
    console.log(` Errors: ${stats.errors}`);
    console.log("-----------------------------");
    
    if (CONFIG.DRY_RUN) {
      console.log("\nThis was a dry run. No users were actually deleted.");
      console.log("Run with --force to actually delete users.");
    }
    
  } catch (error) {
    console.error("Error:", error.message);
    process.exit(1);
  }
}

// Run the main function
(async () => {
  await main();
})();
