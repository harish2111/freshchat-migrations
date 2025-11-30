import fs from "fs";
import path from "path";
import axios from "axios";
import xlsx from "xlsx";
import { config } from "./config.js";
import { randomInt } from "crypto";

const DESTINATION_HEADERS = [
  "sourceUserId",
  "destinationUserId",
  "name",
  "email",
  "phone",
  "Conversation_ids",
];

const destinationWorkbookPath = path.join(
  process.cwd(),
  "data",
  "destination_contacts.xlsx"
);

// --- Create reusable API clients with Authorization headers ---
const sourceApiClient = axios.create({
  baseURL: config.source.domain,
  headers: {
    Authorization: `Bearer ${config.source.apiToken}`,
    "Content-Type": "application/json",
  },
});

const destinationApiClient = axios.create({
  baseURL: config.test.domain,
  headers: {
    Authorization: `Bearer ${config.test.apiToken}`,
    "Content-Type": "application/json",
  },
});

const savedContacts = [];
const contactStore = {};
const migrationResults = [];

function coalesceValue(row, keys = []) {
  for (const key of keys) {
    if (row && row[key] !== undefined && row[key] !== null) {
      return row[key];
    }
  }
  return "";
}

function normalizeExistingRow(row = {}) {
  return {
    sourceUserId: coalesceValue(row, [
      "sourceUserId",
      "SourceUserId",
      "source_user_id",
      "Source User Id",
    ]),
    destinationUserId: coalesceValue(row, [
      "destinationUserId",
      "DestinationUserId",
      "destination_user_id",
      "Destination User Id",
    ]),
    name: coalesceValue(row, ["name", "Name"]),
    email: coalesceValue(row, ["email", "Email"]),
    phone: coalesceValue(row, ["phone", "Phone"]),
    Conversation_ids: coalesceValue(row, [
      "Conversation_ids",
      "conversation_ids",
      "ConversationIds",
      "conversationIds",
      "Conversation Ids",
    ]),
  };
}

function formatNewRow(row) {
  return {
    sourceUserId: row.sourceUserId || "",
    destinationUserId: row.destinationUserId || "",
    name: row.name || "",
    email: row.email || "",
    phone: row.phone || "",
    Conversation_ids:
      row.conversationIds && row.conversationIds.length > 0
        ? row.conversationIds.join(",")
        : "",
  };
}

function writeDestinationContacts(rows = []) {
  if (!rows.length) {
    return;
  }

  let existingRows = [];
  if (fs.existsSync(destinationWorkbookPath)) {
    try {
      const workbook = xlsx.readFile(destinationWorkbookPath);
      const sheetName = workbook.SheetNames[0];
      const sheet = workbook.Sheets[sheetName];
      existingRows = xlsx.utils.sheet_to_json(sheet, { defval: "" });
    } catch (error) {
      console.warn(
        `⚠️ Unable to read existing destination file. A new file will be created. Reason: ${error.message}`
      );
    }
  }

  const normalizedExistingRows = existingRows.map(normalizeExistingRow);
  const formattedNewRows = rows.map(formatNewRow);
  const worksheetData = [...normalizedExistingRows, ...formattedNewRows];

  const worksheet = xlsx.utils.json_to_sheet(worksheetData, {
    header: DESTINATION_HEADERS,
  });

  xlsx.utils.sheet_add_aoa(worksheet, [DESTINATION_HEADERS], { origin: "A1" });

  const workbook = xlsx.utils.book_new();
  xlsx.utils.book_append_sheet(workbook, worksheet, "contacts");
  xlsx.writeFile(workbook, destinationWorkbookPath);
}
async function createContactinDestination(payload) {
  try {
    const response = await destinationApiClient.post(`/v2/users`, payload);

    const created = response.data;

    savedContacts.push({
      id: created.id || null,
      name: created.first_name || null,
      email: created.email || null,
      phone: created.phone || null,
    });

    if (created.id) {
      contactStore[created.id] = {
        name: created.name || null,
        email: created.email || null,
        phone: created.phone || null,
      };
    }

    return created;
  } catch (error) {
    throw error;
  }
}
async function getDestinationUserId(user) {
  try {
    let endPoint = "";
    if (user.email) {
      const encodedEmail = encodeURIComponent(user.email);
      endPoint = `/v2/users?email=${encodedEmail}`;
    } else if (user.phone) {
      endPoint = `/v2/users?phone=${user.phone}`;
    } else {
      console.log(`⚠️ No email and Phone to lookup for the user.`);
      return null;
    }
    const response = await destinationApiClient.get(endPoint);

    if (
      response.data &&
      response.data.users &&
      response.data.users.length > 0
    ) {
      console.log(
        `\n✅ Match found by name (${user.user_alias}) → ID: ${response.data.users[0].id}`
      );
      return response.data.users[0].id;
    } else {
      console.log(
        `No matching for ${user.user_name} | ${user.email} | ${user.phone}`
      );
    }
  } catch (error) {
    console.error(
      `    ❌ Error fetching user by email '${user.email}' from destination:`,
      error.response?.data?.message || error.message
    );
    return null;
  }
}

async function getMessagesForConversation(conversationId) {
  try {
    const response = await sourceApiClient.get(
      `/v2/conversations/${conversationId}/messages?items_per_page=50`
    );
    console.log(
      `      Fetching ${response.data.messages.length} message(s) for conversation ${conversationId}.`
    );
    return response.data.messages || [];
  } catch (error) {
    console.error(
      `      ❌ Error fetching messages for conversation ${conversationId}:`,
      error.response?.data?.message || error.message
    );
    return [];
  }
}

async function createConversationInDestination(payload) {
  try {
    const response = await destinationApiClient.post(
      "/v2/conversations",
      payload
    );
    if (response.status === 202 || response.status === 200) {
      console.log(
        `✅ Successfully created new conversation with ID: ${response.data.conversation_id}`
      );
      return response.data?.conversation_id || null;
    }
    return null;
  } catch (error) {
    console.error(
      `      ❌ Error creating conversation:`,
      error.response?.data?.errors ||
        error.response?.data?.message ||
        error.message
    );
    return null;
  }
}

async function conversationStatusUpdate(conversationId) {
  try {
    const response = await destinationApiClient.put(
      `/v2/conversations/${conversationId}`,
      { status: "resolved" }
    );
  } catch (error) {
    console.log(error);
  }
}
async function processUser(user, destinationUserId) {
  const sourceUserId = user.user_alias;
  const email = user.email || null;
  const phone = user.phone || null;
  const name = user.user_name || user.name || "";
  const resultRow = {
    sourceUserId,
    destinationUserId,
    name,
    email,
    phone,
    conversationIds: [],
  };

  console.log(
    `\n--- Processing Source User ID: ${sourceUserId} (Email: ${email}) | (Phone:${phone}) ---`
  );

  if (destinationUserId === config.test.fixedFallbackUserId) {
    console.log(`-> Using fixed fallback user ID: ${destinationUserId}`);
  }

  try {
    const response1 = await sourceApiClient.get(
      `/v2/users/${sourceUserId}/conversations`
    );

    console.log(
      `    Found ${response1.data.conversations.length} conversation(s) for source user ${sourceUserId}.`
    );

    const conversations = response1.data.conversations.forEach(
      async (convo) => {
        const response = await sourceApiClient.get(
          `/v2/conversations/${convo.id}`
        );
        const conversation = response.data;
        const channel_id =
          randomInt(2) % 2 == 0
            ? config.test.defaultChannelId
            : config.test.defaultChannelId2;
        if (!conversation || !conversation.conversation_id) {
          console.log(
            `No conversation found for source user ${sourceUserId}, skipping.`
          );
          return resultRow;
        }

        // for (const conversation of conversations) {
        console.log(
          `  -> Migrating conversation ID: ${conversation.conversation_id}`
        );

        const messages = await getMessagesForConversation(
          conversation.conversation_id
        );

        if (messages.length === 0) {
          console.log(`No messages found, skipping conversation.`);
          return resultRow;
        }

        // Preserve chronological order (oldest first) so destination UI matches source.
        const sortedMessages = [...messages].sort((a, b) => {
          const aTime = new Date(a.created_time).getTime();
          const bTime = new Date(b.created_time).getTime();
          return aTime - bTime;
        });

        const transformedMessages = await Promise.all(
          sortedMessages
            .filter((msg) => {
              if (msg.message_type === "system") return false;
              return true;
            })
            .map(async (msg) => {
              const newMessage = {};

              if (msg.message_parts && Array.isArray(msg.message_parts)) {
                newMessage.message_parts = msg.message_parts
                  .filter((part) => part.text || part.image || part.video)
                  .map((part) => {
                    if (part.text) return { text: part.text };
                    if (part.image) return { image: part.image };
                    if (part.video) return { video: part.video };
                  });
              }

              if (msg.message_type) newMessage.message_type = msg.message_type;

              if (msg.actor_type && msg.message_type !== "system") {
                newMessage.actor_type = msg.actor_type;
              }

              if (msg.actor_id && msg.message_type !== "system") {
                newMessage.actor_id =
                  msg.actor_id === sourceUserId
                    ? destinationUserId
                    : config.test.fixedActorId;
              }

              if (!msg.actor_id) newMessage.actor_id = config.test.fixedActorId;
              if (!msg.actor_type)
                newMessage.actor_type = config.test.fixedActorType;

              if (msg.channel_id) {
                newMessage.channel_id = channel_id;
              }

              if (msg.created_time) {
                newMessage.created_time = msg.created_time;
              }

              return newMessage;
            })
        );

        const newConversationPayload = {
          status: "new",
          messages: transformedMessages,
          created_time: conversation.created_time,
          users: [{ id: destinationUserId }],
          channel_id: channel_id,
        };
        console.log(JSON.stringify(newConversationPayload));
        // console.log("newConversationPayload", newConversationPayload);
        const destinationConversationId = await createConversationInDestination(
          newConversationPayload
        );

        await conversationStatusUpdate(destinationConversationId);
        await new Promise((resolve) =>
          setTimeout(resolve, config.delayBetweenUsersMs)
        );

        if (destinationConversationId) {
          resultRow.conversationIds.push(destinationConversationId);
        }

        return resultRow;
      }
    );

    return conversations;
  } catch (error) {
    console.error(
      `    ❌ Error fetching conversations for source user ${sourceUserId}:`,
      error.response?.data?.message || error.message
    );
    return [];
  }
}

async function runMigration() {
  console.log("🚀 Starting FreshChat Conversation Migration Script...");

  const excelPath = path.join(process.cwd(), "data", "source_contacts.xlsx");

  if (!fs.existsSync(excelPath)) {
    console.error(
      `\n❌ ERROR: The Excel file was not found at: ${excelPath}\n`
    );
    return;
  }

  try {
    const workbook = xlsx.readFile(excelPath);
    const sheetName = workbook.SheetNames[1];
    const sheet = workbook.Sheets[sheetName];

    const sourceUsers = xlsx.utils.sheet_to_json(sheet);
    console.log(
      `\nFound ${sourceUsers.length} user(s) in the Excel file. Starting migration process...`
    );

    let i = 0;
    for (const user of sourceUsers) {
      if (i >= 200) {
        break;
      }

      let destinationUserId = await getDestinationUserId(user);
      // if (!destinationUserId) {
      //   const contact = await createContactinDestination({
      //     first_name: user.user_name,
      //     email: user.email || null,
      //     phone: user.phone || null,
      //     properties: [
      //       {
      //         name: "cf_old_alias",
      //         value: user.user_alias,
      //       },
      //     ],
      //   });
      //   destinationUserId = contact.id;
      // }

      // const processedRow = await processUser(user, destinationUserId);
      // if (processedRow) {
      //   migrationResults.push(processedRow);
      // }
      // await new Promise((resolve) =>
      //   setTimeout(resolve, config.delayBetweenUsersMs)
      // );
      i++;
    }

    // writeDestinationContacts(migrationResults);
    console.log("\n✅ Migration process finished!");
    return migrationResults;
  } catch (error) {
    console.log(error);
    console.error("\n❌ ERROR reading Excel file:", error.message);
  }
}

// --- Start the script ---
runMigration();
